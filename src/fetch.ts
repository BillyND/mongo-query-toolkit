/**
 * @file Fetch utilities for MongoDB
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
// Fetch List
// ============================================================================

/**
 * Fetch paginated list from MongoDB
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
  const { searchParams } = new URL(url);
  const {
    limit: maxLimit = 250,
    sortField = "updatedAt",
    sortDir = "desc",
    tenantField = "shopDomain",
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
  const isExporting = !!searchParams.get("export");
  const countOnly = !!searchParams.get("countResultOnly");

  // Build filter pipeline
  const filters = getFiltersFromUrl(url, tenantValue, tenantField);
  const filterPipeline = await buildPipelineWithPercent(filters, model);

  const pipeline: PipelineStage[] = [...initialPipeline, ...filterPipeline];

  // Get total
  const total =
    (await model.aggregate([...pipeline, { $count: "total" }]).exec())?.[0]
      ?.total || 0;

  if (countOnly) {
    return { page, total, items: [] };
  }

  // Add sort
  const hasSortStage = pipeline.some((s) => "$sort" in s);
  if (!hasSortStage) {
    const field = sortBy || sortField;
    const dir =
      sortDirection?.toLowerCase() === "asc"
        ? 1
        : sortDirection?.toLowerCase() === "desc"
        ? -1
        : sortDir === "asc"
        ? 1
        : -1;
    pipeline.push({ $sort: { [field]: dir } });
  }

  // Add final pipeline
  if (finalPipeline.length) {
    pipeline.push(...finalPipeline);
  }

  // Add pagination
  if (!isExporting && limit > 0) {
    pipeline.push({ $skip: skip }, { $limit: limit });
  }

  const items = (await model.aggregate(pipeline).exec()) as T[];

  return { page, total, items };
}

// ============================================================================
// Fetch Unified List (Multi-Model)
// ============================================================================

/**
 * Fetch from multiple models using $unionWith
 */
export async function fetchUnifiedList<T = Record<string, unknown>>(
  request: RequestInput,
  models: MultiModelConfig[],
  options: FetchOptions = {}
): Promise<FetchListResult<T>> {
  if (!models.length) return { page: 1, total: 0, items: [] };

  const url = getUrlFromRequest(request);
  const { searchParams } = new URL(url);
  const {
    limit: maxLimit = 250,
    sortField = "updatedAt",
    sortDir = "desc",
    tenantField = "shopDomain",
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
  const isExporting = !!searchParams.get("export");
  const countOnly = !!searchParams.get("countResultOnly");

  const [baseModel, ...otherModels] = models;
  const filters = getFiltersFromUrl(url, tenantValue, tenantField);
  const filterPipeline = buildPipeline(filters);

  // Build base pipeline
  const pipeline: PipelineStage[] = [
    ...(baseModel.initialPipeline || []),
    ...filterPipeline,
    { $addFields: { _sourceType: baseModel.model.collection.name } },
  ];

  // Add $unionWith for other models
  for (const cfg of otherModels) {
    const unionPipeline: PipelineStage[] = [
      ...(cfg.initialPipeline || []),
      ...filterPipeline,
      { $addFields: { _sourceType: cfg.model.collection.name } },
      ...(cfg.finalPipeline || []),
    ];
    pipeline.push({
      $unionWith: { coll: cfg.model.collection.name, pipeline: unionPipeline },
    } as PipelineStage);
  }

  if (baseModel.finalPipeline?.length) {
    pipeline.push(...baseModel.finalPipeline);
  }

  // Get total
  const total =
    (
      await baseModel.model.aggregate([...pipeline, { $count: "total" }]).exec()
    )?.[0]?.total || 0;

  if (countOnly) return { page, total, items: [] };

  // Add sort
  const field = sortBy || sortField;
  const dir =
    sortDirection?.toLowerCase() === "asc"
      ? 1
      : sortDirection?.toLowerCase() === "desc"
      ? -1
      : sortDir === "asc"
      ? 1
      : -1;
  pipeline.push({ $sort: { [field]: dir } });

  // Add pagination
  if (!isExporting && limit > 0) {
    pipeline.push({ $skip: skip }, { $limit: limit });
  }

  const items = (await baseModel.model.aggregate(pipeline).exec()) as T[];
  return { page, total, items };
}

// ============================================================================
// Fetch Item
// ============================================================================

/**
 * Fetch single item by ID
 *
 * @example
 * ```ts
 * const item = await fetchItem(request, MyModel);
 * // or with explicit ID
 * const item = await fetchItem(request, MyModel, [], [], "123");
 * ```
 */
export async function fetchItem<T = Record<string, unknown>>(
  request: RequestInput,
  model: Model<unknown>,
  initialPipeline: PipelineStage[] = [],
  finalPipeline: PipelineStage[] = [],
  id?: string | number
): Promise<T | null> {
  // Get ID from URL or param
  if (!id) {
    const url = getUrlFromRequest(request);
    const { searchParams } = new URL(url);
    id = searchParams.get("id") ?? undefined;
  }

  if (!id) return null;

  const idStr = String(id);

  const pipeline: PipelineStage[] = [
    ...initialPipeline,
    {
      $match: {
        $or: [
          { id: idStr },
          { id: Number(idStr) },
          { _id: ObjectId.isValid(idStr) ? new ObjectId(idStr) : idStr },
        ],
      },
    },
    ...finalPipeline,
    { $limit: 1 },
  ];

  const result = await model.aggregate(pipeline).exec();
  return (result?.[0] as T) ?? null;
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
  const result = await model
    .aggregate([...pipeline, { $match: { [field]: value } }, { $limit: 1 }])
    .exec();
  return (result?.[0] as T) ?? null;
}
