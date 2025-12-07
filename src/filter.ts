/**
 * @file Filter utilities for MongoDB aggregation
 * @description Optimized for maximum performance
 */

import type { PipelineStage, Model } from "mongoose";
import mongoose from "mongoose";
import type {
  FilterValue,
  FilterType,
  BetweenOperatorValue,
  RequestInput,
  QueryUrlOptions,
} from "./types.js";

const { ObjectId } = mongoose.Types;

// ============================================================================
// Cache for ObjectId validation (avoid repeated validation)
// ============================================================================

const objectIdCache = new Map<string, boolean>();
const MAX_CACHE_SIZE = 1000;

function isValidObjectId(str: string): boolean {
  let result = objectIdCache.get(str);
  if (result === undefined) {
    result = ObjectId.isValid(str);
    // Limit cache size to prevent memory leak
    if (objectIdCache.size >= MAX_CACHE_SIZE) {
      // Clear first half of cache
      const keys = Array.from(objectIdCache.keys());
      for (let i = 0; i < MAX_CACHE_SIZE / 2; i++) {
        objectIdCache.delete(keys[i]);
      }
    }
    objectIdCache.set(str, result);
  }
  return result;
}

// ============================================================================
// Helper
// ============================================================================

/**
 * Extract URL string from Request or string
 */
export function getUrlFromRequest(input: RequestInput): string {
  return typeof input === "string" ? input : input.url;
}

// ============================================================================
// Constants
// ============================================================================

const FORCE_STRING_FIELDS = new Set(["name", "id"]);
const FORCE_EQUAL_FIELDS = new Set(["id", "_id"]);

const SUPPORTED_OPERATORS: Record<string, Set<FilterType>> = {
  eq: new Set(["string", "date", "amount", "array"]),
  ne: new Set(["string", "amount", "array"]),
  has: new Set(["string"]),
  nh: new Set(["string"]),
  any: new Set(["string", "array"]),
  none: new Set(["string", "array"]),
  range: new Set(["date", "amount"]),
  lt: new Set(["amount"]),
  gt: new Set(["amount"]),
  before: new Set(["date"]),
  after: new Set(["date"]),
};

// ============================================================================
// Date Helpers - Optimized
// ============================================================================

const DATE_ONLY_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const DATE_RANGE_REGEX = /^\d{4}-\d{2}-\d{2}(~\d{4}-\d{2}-\d{2})?$/;
const NUMERIC_REGEX = /^[\d.]+$/;

function toDate(input: string | Date): Date {
  if (input instanceof Date) return input;
  if (DATE_ONLY_REGEX.test(input)) {
    return new Date(`${input}T00:00:00.000Z`);
  }
  return new Date(input);
}

function startOfDay(input: string | Date): Date {
  const d = toDate(input);
  const iso = d.toISOString();
  return new Date(`${iso.slice(0, 10)}T00:00:00.000Z`);
}

function endOfDay(input: string | Date): Date {
  const d = toDate(input);
  const iso = d.toISOString();
  return new Date(`${iso.slice(0, 10)}T23:59:59.999Z`);
}

// ============================================================================
// Parse Filter String - Optimized
// ============================================================================

/**
 * Parse filter string: "field|type|operator|value" or "field|value"
 * OPTIMIZED: Reduced string operations, cached regex
 */
export function parseFilter(param: string): FilterValue {
  const parts = param.split("|");
  const field = parts[0];
  const p2 = parts[1];
  const p3 = parts[2];
  const p4 = parts[3];
  const percentOfResult = parts[4];

  let type: FilterType;
  let operator: string;
  let value: string | BetweenOperatorValue;

  // Short format: field|value or field|type|value
  if (p4 === undefined && (p3 !== undefined || p2 !== undefined)) {
    value = p3 || p2;

    // Detect type
    if (DATE_RANGE_REGEX.test(value)) {
      type = "date";
    } else if (NUMERIC_REGEX.test(value) && !FORCE_STRING_FIELDS.has(field)) {
      type = "amount";
    } else {
      type = "string";
    }

    // Detect operator
    if (value.includes(",")) {
      type = "array";
      operator = "any";
    } else if (value.includes("~")) {
      operator = "range";
    } else if (FORCE_EQUAL_FIELDS.has(field)) {
      operator = "eq";
    } else {
      operator = "has";
    }
  } else {
    // Full format: field|type|operator|value
    type = p2 as FilterType;
    operator = p3;
    value = p4;
  }

  // Parse range value
  if (operator === "range" && typeof value === "string") {
    const tildeIdx = value.indexOf("~");
    if (tildeIdx !== -1) {
      value = { from: value.slice(0, tildeIdx), to: value.slice(tildeIdx + 1) };
    }
  }

  return {
    field,
    type,
    operator: operator as FilterValue["operator"],
    value,
    percentOfResult,
  };
}

/**
 * Parse multiple filter strings
 */
export function parseFilters(params: string[]): FilterValue[] {
  const result: FilterValue[] = new Array(params.length);
  for (let i = 0; i < params.length; i++) {
    result[i] = parseFilter(params[i]);
  }
  return result;
}

// ============================================================================
// Build Pipeline Stage - Optimized
// ============================================================================

function prepareValue(
  value: string | BetweenOperatorValue,
  type: FilterType,
  operator: string
): unknown {
  if (type === "date") {
    if (operator === "range") {
      const v = value as BetweenOperatorValue;
      return [startOfDay(v.from), endOfDay(v.to)];
    }
    return [startOfDay(value as string), endOfDay(value as string)];
  }
  if (type === "amount") {
    if (operator === "range") {
      const v = value as BetweenOperatorValue;
      return [parseFloat(v.from), parseFloat(v.to)];
    }
    return parseFloat(value as string);
  }
  return value;
}

/**
 * Build match stage for a filter
 * OPTIMIZED: Reduced object creation, cached ObjectId validation
 */
function buildMatch(filter: FilterValue): PipelineStage | null {
  const { field, type, operator, value } = filter;
  if (!field || !type || !operator || value === undefined) return null;

  const supportedTypes = SUPPORTED_OPERATORS[operator];
  if (!supportedTypes || !supportedTypes.has(type)) return null;

  const v = prepareValue(value, type, operator);

  switch (operator) {
    case "eq":
      if (type === "date") {
        const dates = v as Date[];
        return {
          $match: {
            [field]: { $gte: dates[0], $lte: dates[1] },
          },
        };
      }
      if (type === "string") {
        if (field === "id" || field === "_id") {
          const conditions: Record<string, unknown>[] = [{ id: v }];
          const numVal = Number(v);
          if (!Number.isNaN(numVal)) {
            conditions.push({ id: numVal });
          }
          if (isValidObjectId(v as string)) {
            conditions.push({ _id: new ObjectId(v as string) });
          }
          return { $match: { $or: conditions } };
        }
        return { $match: { [field]: { $regex: new RegExp(`^${v}$`, "i") } } };
      }
      return { $match: { [field]: v } };

    case "ne":
      if (type === "string") {
        return { $match: { [field]: { $not: new RegExp(`^${v}$`, "i") } } };
      }
      return { $match: { [field]: { $ne: v } } };

    case "has":
      return { $match: { [field]: { $regex: new RegExp(v as string, "i") } } };

    case "nh":
      return { $match: { [field]: { $not: new RegExp(v as string, "i") } } };

    case "any":
      if (type === "array") {
        const strVal = v as string;
        const parts = strVal.split(",");
        const vals: (string | mongoose.Types.ObjectId)[] = new Array(parts.length);
        for (let i = 0; i < parts.length; i++) {
          const x = parts[i];
          vals[i] = isValidObjectId(x) ? new ObjectId(x) : x;
        }
        return { $match: { [field]: { $in: vals } } };
      }
      return {
        $match: {
          [field]: {
            $regex: new RegExp(`^(${(v as string).split(",").join("|")})$`, "i"),
          },
        },
      };

    case "none":
      if (type === "array") {
        const strVal = v as string;
        const parts = strVal.split(",");
        const vals: (string | mongoose.Types.ObjectId)[] = new Array(parts.length);
        for (let i = 0; i < parts.length; i++) {
          const x = parts[i];
          vals[i] = isValidObjectId(x) ? new ObjectId(x) : x;
        }
        return { $match: { [field]: { $nin: vals } } };
      }
      return {
        $match: {
          [field]: {
            $not: new RegExp(`^(${(v as string).split(",").join("|")})$`, "i"),
          },
        },
      };

    case "range": {
      const rangeVals = v as unknown[];
      return {
        $match: {
          [field]: { $gte: rangeVals[0], $lte: rangeVals[1] },
        },
      };
    }

    case "lt":
    case "before":
      return {
        $match: { [field]: { $lt: type === "date" ? (v as Date[])[0] : v } },
      };

    case "gt":
    case "after":
      return {
        $match: { [field]: { $gt: type === "date" ? (v as Date[])[1] : v } },
      };

    default:
      return null;
  }
}

// ============================================================================
// Build Full Pipeline - Optimized
// ============================================================================

/**
 * Build MongoDB pipeline from filter values
 * OPTIMIZED: Pre-allocate array, avoid spread
 */
export function buildPipeline(filters: FilterValue[]): PipelineStage[] {
  const pipeline: PipelineStage[] = [];
  for (let i = 0; i < filters.length; i++) {
    const stage = buildMatch(filters[i]);
    if (stage) pipeline.push(stage);
  }
  return pipeline;
}

/**
 * Build pipeline with percentage-based limit support
 * OPTIMIZED: Batch percentOfResult filters, minimize DB calls
 */
export async function buildPipelineWithPercent(
  filters: FilterValue[],
  model: Model<unknown>
): Promise<PipelineStage[]> {
  const pipeline: PipelineStage[] = [];

  // First pass: build all match stages
  const filtersWithPercent: Array<{ index: number; percent: number }> = [];

  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    const stage = buildMatch(filter);
    if (stage) {
      pipeline.push(stage);

      // Track filters that need percent calculation
      if (filter.percentOfResult) {
        const pct = parseFloat(String(filter.percentOfResult));
        if (!Number.isNaN(pct) && pct !== 0) {
          filtersWithPercent.push({ index: pipeline.length - 1, percent: pct });
        }
      }
    }
  }

  // If no percent filters, return early
  if (filtersWithPercent.length === 0) {
    return pipeline;
  }

  // For percent filters, we need to get the total count ONCE at the end
  // and apply the percentage limit/skip
  // Note: This is a simplified approach - for complex cases with multiple
  // percent filters, we apply the last one
  const lastPercent = filtersWithPercent[filtersWithPercent.length - 1];
  const pct = lastPercent.percent;

  // Get count with current pipeline
  const countResult = await model
    .aggregate([...pipeline, { $count: "total" }])
    .allowDiskUse(true)
    .exec();

  const total = countResult[0]?.total || 0;

  if (total > 0) {
    const num =
      pct >= 0
        ? Math.ceil((total / 100) * pct)
        : Math.floor((total / 100) * -pct);

    if (pct < 0) {
      pipeline.push({ $skip: total - num });
    }
    pipeline.push({ $limit: num });
  }

  return pipeline;
}

/**
 * Extract filter strings from Request or URL
 * OPTIMIZED: Single URL parse, efficient array operations
 */
export function getFiltersFromUrl(
  request: RequestInput,
  tenantValue?: string,
  tenantField?: string
): FilterValue[] {
  const url = getUrlFromRequest(request);
  const urlObj = new URL(url);
  const filterStrings = urlObj.searchParams.getAll("filter");

  // Pre-calculate capacity
  const hasTenant = !!(tenantField && tenantValue);
  const capacity = filterStrings.length + (hasTenant ? 1 : 0);
  const filters: FilterValue[] = new Array(capacity);

  let idx = 0;

  // Add tenant filter first if both field and value are provided
  if (hasTenant) {
    filters[idx++] = {
      field: tenantField!,
      type: "string",
      operator: "eq",
      value: tenantValue,
    };
  }

  // Parse filter strings, excluding tenant field if already added
  for (let i = 0; i < filterStrings.length; i++) {
    const f = filterStrings[i];
    if (!tenantField || !f.includes(tenantField)) {
      filters[idx++] = parseFilter(f);
    }
  }

  // Trim array to actual size
  filters.length = idx;
  return filters;
}

// ============================================================================
// Client-Side URL Builder - Optimized
// ============================================================================

/**
 * Build a query URL with filters, pagination, and sorting.
 * Works in both browser and Node.js environments.
 * OPTIMIZED: Direct string building for common cases
 *
 * @example
 * ```ts
 * const url = buildQueryUrl("/api/products", {
 *   page: 1,
 *   limit: 20,
 *   sort: "price|desc",
 *   filters: ["status|string|eq|active", "price|amount|gt|100"],
 * });
 * // => "/api/products?page=1&limit=20&sort=price|desc&filter=status|string|eq|active&filter=price|amount|gt|100"
 * ```
 */
export function buildQueryUrl(
  baseUrl: string,
  options: QueryUrlOptions = {}
): string {
  const parts: string[] = [];

  if (options.page) parts.push(`page=${options.page}`);
  if (options.limit) parts.push(`limit=${options.limit}`);
  if (options.sort) parts.push(`sort=${encodeURIComponent(options.sort)}`);
  if (options.id) parts.push(`id=${encodeURIComponent(options.id)}`);
  if (options.export) parts.push("export=true");
  if (options.countOnly) parts.push("countResultOnly=true");

  const filters = options.filters;
  if (filters) {
    for (let i = 0; i < filters.length; i++) {
      parts.push(`filter=${encodeURIComponent(filters[i])}`);
    }
  }

  if (parts.length === 0) return baseUrl;
  return `${baseUrl}?${parts.join("&")}`;
}
