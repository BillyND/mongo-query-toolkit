/**
 * @file Fetch utilities for MongoDB
 * @description Hybrid approach: Uses optimal strategy based on query complexity
 * - With filters: Single $facet query (efficient for filtered data)
 * - Without filters: Parallel queries (faster for full collection scan)
 */

import type { Model, PipelineStage } from "mongoose";
import mongoose from "mongoose";
import type {
  FetchListResult,
  FetchOptions,
  MultiModelConfig,
  RequestInput,
} from "./types.js";
import {
  getFiltersFromUrl,
  getUrlFromRequest,
  buildPipeline,
  buildPipelineWithPercent,
} from "./filter.js";

const { ObjectId } = mongoose.Types;

// ============================================================================
// Fetch List - HYBRID: Optimal strategy based on query complexity
// ============================================================================

/**
 * Fetch paginated list from MongoDB
 * HYBRID APPROACH:
 * - With filters → $facet (1 query, efficient for filtered data)
 * - Without filters → 2 parallel queries (faster for full collection)
 *
 * @example
 * ```ts
 * const result = await fetchList(request, MyModel, {
 *   tenantValue: shopDomain,
 *   limit: 50,
 * });
 * ```
 */
export async function fetchList<T = Record<string, unknown>>(
  request: RequestInput,
  model: Model<unknown>,
  options: FetchOptions = {},
  initialPipeline: PipelineStage[] = [],
  finalPipeline: PipelineStage[] = []
): Promise<FetchListResult<T>> {
  const url = getUrlFromRequest(request);
  const urlObj = new URL(url);
  const searchParams = urlObj.searchParams;

  const {
    limit: maxLimit = 250,
    sortField = "updatedAt",
    sortDir = "desc",
    tenantField,
    tenantValue,
  } = options;

  // Parse pagination
  const limit = Math.min(
    Number(searchParams.get("limit") || maxLimit),
    maxLimit
  );
  const page = Math.max(Number(searchParams.get("page") || 1), 1);
  const skip = (page - 1) * limit;

  // Parse sort
  const sortParam = searchParams.get("sort");
  const [sortBy, sortDirection] = sortParam?.split("|") || [];

  // Check flags
  const isExporting = searchParams.has("export");
  const countOnly = searchParams.has("countResultOnly");

  // Build filter pipeline
  const filters = getFiltersFromUrl(url, tenantValue, tenantField);
  const filterPipeline = await buildPipelineWithPercent(filters, model);

  // Check if we have filters (determines which strategy to use)
  const hasFilters = filterPipeline.length > 0 || initialPipeline.length > 0;

  // Build base pipeline
  const pipeline: PipelineStage[] = [];
  for (let i = 0; i < initialPipeline.length; i++) {
    pipeline.push(initialPipeline[i]);
  }
  for (let i = 0; i < filterPipeline.length; i++) {
    pipeline.push(filterPipeline[i]);
  }

  // Determine sort
  let sortStage: PipelineStage | null = null;
  let hasSortStage = false;
  for (let i = 0; i < pipeline.length; i++) {
    if ("$sort" in pipeline[i]) {
      hasSortStage = true;
      break;
    }
  }

  if (!hasSortStage) {
    const field = sortBy || sortField;
    const dir =
      sortDirection === "asc"
        ? 1
        : sortDirection === "desc"
        ? -1
        : sortDir === "asc"
        ? 1
        : -1;
    sortStage = { $sort: { [field]: dir } };
  }

  // Count only mode - single count query
  if (countOnly) {
    let total: number;

    if (hasFilters) {
      const countResult = await model
        .aggregate([...pipeline, { $count: "total" }])
        .allowDiskUse(true)
        .exec();
      total = countResult[0]?.total || 0;
    } else {
      // No filters - use fast estimatedDocumentCount
      total = await model.estimatedDocumentCount().exec();
    }

    const totalPages = limit > 0 ? Math.ceil(total / limit) : 0;

    return {
      page,
      limit,
      total,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
      nextPage: page < totalPages ? page + 1 : null,
      prevPage: page > 1 ? page - 1 : null,
      items: [],
    };
  }

  // ========================================================================
  // HYBRID STRATEGY: Choose optimal approach based on filters
  // ========================================================================

  if (hasFilters) {
    // WITH FILTERS: Use $facet (1 query) - efficient for filtered data
    const itemsPipeline: PipelineStage[] = [];
    if (sortStage) itemsPipeline.push(sortStage);
    for (let i = 0; i < finalPipeline.length; i++) {
      itemsPipeline.push(finalPipeline[i]);
    }
    if (!isExporting && limit > 0) {
      itemsPipeline.push({ $skip: skip }, { $limit: limit });
    }

    const facetStage = {
      $facet: {
        metadata: [{ $count: "total" }],
        items: itemsPipeline,
      },
    } as PipelineStage;

    pipeline.push(facetStage);

    const facetResult = await model
      .aggregate(pipeline)
      .allowDiskUse(true)
      .exec();

    const result = facetResult[0];
    const total = result?.metadata[0]?.total || 0;
    const items = (result?.items || []) as T[];

    const totalPages = limit > 0 ? Math.ceil(total / limit) : 0;

    return {
      page,
      limit,
      total,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
      nextPage: page < totalPages ? page + 1 : null,
      prevPage: page > 1 ? page - 1 : null,
      items,
    };
  } else {
    // NO FILTERS: Use 2 parallel queries - faster for full collection scan
    const itemsPipeline: PipelineStage[] = [...pipeline];
    if (sortStage) itemsPipeline.push(sortStage);
    for (let i = 0; i < finalPipeline.length; i++) {
      itemsPipeline.push(finalPipeline[i]);
    }
    if (!isExporting && limit > 0) {
      itemsPipeline.push({ $skip: skip }, { $limit: limit });
    }

    // Run count and items in parallel
    const [total, items] = await Promise.all([
      model.estimatedDocumentCount().exec(),
      model.aggregate(itemsPipeline).allowDiskUse(true).exec() as Promise<T[]>,
    ]);

    const totalPages = limit > 0 ? Math.ceil(total / limit) : 0;

    return {
      page,
      limit,
      total,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
      nextPage: page < totalPages ? page + 1 : null,
      prevPage: page > 1 ? page - 1 : null,
      items,
    };
  }
}

// ============================================================================
// Fetch Unified List (Multi-Model) - HYBRID
// ============================================================================

/**
 * Fetch from multiple models using $unionWith
 * Always uses $facet since $unionWith requires aggregation
 */
export async function fetchUnifiedList<T = Record<string, unknown>>(
  request: RequestInput,
  models: MultiModelConfig[],
  options: FetchOptions = {}
): Promise<FetchListResult<T>> {
  if (!models.length) {
    return {
      page: 1,
      limit: 0,
      total: 0,
      totalPages: 0,
      hasNextPage: false,
      hasPrevPage: false,
      nextPage: null,
      prevPage: null,
      items: [],
    };
  }

  const url = getUrlFromRequest(request);
  const urlObj = new URL(url);
  const searchParams = urlObj.searchParams;

  const {
    limit: maxLimit = 250,
    sortField = "updatedAt",
    sortDir = "desc",
    tenantField,
    tenantValue,
  } = options;

  const limit = Math.min(
    Number(searchParams.get("limit") || maxLimit),
    maxLimit
  );
  const page = Math.max(Number(searchParams.get("page") || 1), 1);
  const skip = (page - 1) * limit;
  const sortParam = searchParams.get("sort");
  const [sortBy, sortDirection] = sortParam?.split("|") || [];
  const isExporting = searchParams.has("export");
  const countOnly = searchParams.has("countResultOnly");

  const [baseModel, ...otherModels] = models;
  const filters = getFiltersFromUrl(url, tenantValue, tenantField ?? "");
  const filterPipeline = buildPipeline(filters);

  // Build base pipeline
  const pipeline: PipelineStage[] = [];

  const baseInitial = baseModel.initialPipeline;
  if (baseInitial) {
    for (let i = 0; i < baseInitial.length; i++) {
      pipeline.push(baseInitial[i]);
    }
  }

  for (let i = 0; i < filterPipeline.length; i++) {
    pipeline.push(filterPipeline[i]);
  }

  pipeline.push({
    $addFields: { _sourceType: baseModel.model.collection.name },
  });

  // Add $unionWith for other models
  for (let i = 0; i < otherModels.length; i++) {
    const cfg = otherModels[i];
    const unionPipeline: PipelineStage[] = [];

    const cfgInitial = cfg.initialPipeline;
    if (cfgInitial) {
      for (let j = 0; j < cfgInitial.length; j++) {
        unionPipeline.push(cfgInitial[j]);
      }
    }

    for (let j = 0; j < filterPipeline.length; j++) {
      unionPipeline.push(filterPipeline[j]);
    }

    unionPipeline.push({
      $addFields: { _sourceType: cfg.model.collection.name },
    });

    const cfgFinal = cfg.finalPipeline;
    if (cfgFinal) {
      for (let j = 0; j < cfgFinal.length; j++) {
        unionPipeline.push(cfgFinal[j]);
      }
    }

    pipeline.push({
      $unionWith: { coll: cfg.model.collection.name, pipeline: unionPipeline },
    } as PipelineStage);
  }

  const baseFinal = baseModel.finalPipeline;
  if (baseFinal) {
    for (let i = 0; i < baseFinal.length; i++) {
      pipeline.push(baseFinal[i]);
    }
  }

  // Determine sort
  const field = sortBy || sortField;
  const dir =
    sortDirection === "asc"
      ? 1
      : sortDirection === "desc"
      ? -1
      : sortDir === "asc"
      ? 1
      : -1;
  const sortStage: PipelineStage = { $sort: { [field]: dir } };

  // Count only mode
  if (countOnly) {
    const countResult = await baseModel.model
      .aggregate([...pipeline, { $count: "total" }])
      .allowDiskUse(true)
      .exec();

    const total = countResult[0]?.total || 0;
    const totalPages = limit > 0 ? Math.ceil(total / limit) : 0;

    return {
      page,
      limit,
      total,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
      nextPage: page < totalPages ? page + 1 : null,
      prevPage: page > 1 ? page - 1 : null,
      items: [],
    };
  }

  // Build items pipeline
  const itemsPipeline: PipelineStage[] = [sortStage];
  if (!isExporting && limit > 0) {
    itemsPipeline.push({ $skip: skip }, { $limit: limit });
  }

  // Use $facet for unified list (required due to $unionWith)
  const facetStage = {
    $facet: {
      metadata: [{ $count: "total" }],
      items: itemsPipeline,
    },
  } as PipelineStage;

  pipeline.push(facetStage);

  const facetResult = await baseModel.model
    .aggregate(pipeline)
    .allowDiskUse(true)
    .exec();

  const result = facetResult[0];
  const total = result?.metadata[0]?.total || 0;
  const items = (result?.items || []) as T[];

  const totalPages = limit > 0 ? Math.ceil(total / limit) : 0;

  return {
    page,
    limit,
    total,
    totalPages,
    hasNextPage: page < totalPages,
    hasPrevPage: page > 1,
    nextPage: page < totalPages ? page + 1 : null,
    prevPage: page > 1 ? page - 1 : null,
    items,
  };
}

// ============================================================================
// Fetch Item - OPTIMIZED
// ============================================================================

/**
 * Fetch single item by ID
 * OPTIMIZED: Early return, simplified ID matching
 */
export async function fetchItem<T = Record<string, unknown>>(
  request: RequestInput,
  model: Model<unknown>,
  initialPipeline: PipelineStage[] = [],
  finalPipeline: PipelineStage[] = [],
  id?: string | number
): Promise<T | null> {
  let idStr: string | undefined;

  if (id !== undefined) {
    idStr = String(id);
  } else {
    const url = getUrlFromRequest(request);
    const urlObj = new URL(url);
    idStr = urlObj.searchParams.get("id") ?? undefined;
  }

  if (!idStr) return null;

  // Build $or conditions for ID matching
  const idConditions: Record<string, unknown>[] = [{ id: idStr }];

  const numId = Number(idStr);
  if (!Number.isNaN(numId)) {
    idConditions.push({ id: numId });
  }

  if (ObjectId.isValid(idStr)) {
    idConditions.push({ _id: new ObjectId(idStr) });
  }

  // Build pipeline efficiently
  const pipeline: PipelineStage[] = [];

  for (let i = 0; i < initialPipeline.length; i++) {
    pipeline.push(initialPipeline[i]);
  }

  if (idConditions.length === 1) {
    pipeline.push({ $match: idConditions[0] });
  } else {
    pipeline.push({ $match: { $or: idConditions } });
  }

  for (let i = 0; i < finalPipeline.length; i++) {
    pipeline.push(finalPipeline[i]);
  }

  pipeline.push({ $limit: 1 });

  const result = await model.aggregate(pipeline).exec();
  return (result[0] as T) ?? null;
}

/**
 * Fetch item by field value
 */
export async function fetchItemBy<T = Record<string, unknown>>(
  model: Model<unknown>,
  field: string,
  value: unknown,
  pipeline: PipelineStage[] = []
): Promise<T | null> {
  const fullPipeline: PipelineStage[] = [];

  for (let i = 0; i < pipeline.length; i++) {
    fullPipeline.push(pipeline[i]);
  }

  fullPipeline.push({ $match: { [field]: value } }, { $limit: 1 });

  const result = await model.aggregate(fullPipeline).exec();
  return (result[0] as T) ?? null;
}
