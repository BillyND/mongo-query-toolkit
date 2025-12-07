/**
 * @file Filter utilities for MongoDB aggregation
 */

import type { PipelineStage, Model } from "mongoose";
import mongoose from "mongoose";
import type {
  FilterValue,
  FilterType,
  BetweenOperatorValue,
  RequestInput,
} from "./types.js";

const { ObjectId } = mongoose.Types;

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

const FORCE_STRING_FIELDS = ["name", "id", "cancellationType"];
const FORCE_EQUAL_FIELDS = ["id", "_id"];

const SUPPORTED_OPERATORS: Record<string, FilterType[]> = {
  eq: ["string", "date", "amount", "array"],
  ne: ["string", "amount", "array"],
  has: ["string"],
  nh: ["string"],
  any: ["string", "array"],
  none: ["string", "array"],
  range: ["date", "amount"],
  lt: ["amount"],
  gt: ["amount"],
  before: ["date"],
  after: ["date"],
};

// ============================================================================
// Date Helpers
// ============================================================================

function toDate(input: string | Date): Date {
  if (input instanceof Date) return input;
  if (/^\d{4}-\d{2}-\d{2}$/.test(input)) {
    return new Date(`${input}T00:00:00.000Z`);
  }
  return new Date(input);
}

function startOfDay(input: string | Date): Date {
  const d = toDate(input);
  return new Date(`${d.toISOString().split("T")[0]}T00:00:00.000Z`);
}

function endOfDay(input: string | Date): Date {
  const d = toDate(input);
  return new Date(`${d.toISOString().split("T")[0]}T23:59:59.999Z`);
}

// ============================================================================
// Parse Filter String
// ============================================================================

/**
 * Parse filter string: "field|type|operator|value" or "field|value"
 */
export function parseFilter(param: string): FilterValue {
  const [field, p2, p3, p4, percentOfResult] = param.split("|", 5);
  let type: FilterType;
  let operator: string;
  let value: string | BetweenOperatorValue;

  // Short format: field|value
  if (p4 === undefined && (p3 !== undefined || p2 !== undefined)) {
    value = p3 || p2;

    // Detect type
    if (/^\d{4}-\d{2}-\d{2}(~\d{4}-\d{2}-\d{2})?$/.test(value)) {
      type = "date";
    } else if (/^[\d.]+$/.test(value) && !FORCE_STRING_FIELDS.includes(field)) {
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
    } else if (FORCE_EQUAL_FIELDS.includes(field)) {
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
  if (
    operator === "range" &&
    typeof value === "string" &&
    value.includes("~")
  ) {
    const [from, to] = value.split("~", 2);
    value = { from, to };
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
  return params.map(parseFilter);
}

// ============================================================================
// Build Pipeline Stage
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

function buildMatch(filter: FilterValue): PipelineStage | null {
  const { field, type, operator, value } = filter;
  if (!field || !type || !operator || value === undefined) return null;
  if (!SUPPORTED_OPERATORS[operator]?.includes(type)) return null;

  const v = prepareValue(value, type, operator);

  switch (operator) {
    case "eq":
      if (type === "date") {
        return {
          $match: {
            $and: [
              { [field]: { $gte: (v as Date[])[0] } },
              { [field]: { $lte: (v as Date[])[1] } },
            ],
          },
        };
      }
      if (type === "string") {
        if (["id", "_id"].includes(field)) {
          return {
            $match: {
              $or: [
                { id: v },
                { id: Number(v) },
                {
                  _id: ObjectId.isValid(v as string)
                    ? new ObjectId(v as string)
                    : v,
                },
              ],
            },
          };
        }
        return { $match: { [field]: { $regex: new RegExp(`^${v}$`, "i") } } };
      }
      return { $match: { [field]: { $eq: v } } };

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
        const vals =
          typeof v === "string"
            ? v
                .split(",")
                .map((x) => (ObjectId.isValid(x) ? new ObjectId(x) : x))
            : [v];
        return { $match: { [field]: { $in: vals } } };
      }
      return {
        $match: {
          [field]: {
            $regex: new RegExp(
              `^(${(v as string).split(",").join("|")})$`,
              "i"
            ),
          },
        },
      };

    case "none":
      if (type === "array") {
        const vals =
          typeof v === "string"
            ? v
                .split(",")
                .map((x) => (ObjectId.isValid(x) ? new ObjectId(x) : x))
            : [v];
        return { $match: { [field]: { $nin: vals } } };
      }
      return {
        $match: {
          [field]: {
            $not: new RegExp(`^(${(v as string).split(",").join("|")})$`, "i"),
          },
        },
      };

    case "range":
      return {
        $match: {
          $and: [
            { [field]: { $gte: (v as unknown[])[0] } },
            { [field]: { $lte: (v as unknown[])[1] } },
          ],
        },
      };

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
// Build Full Pipeline
// ============================================================================

/**
 * Build MongoDB pipeline from filter values
 */
export function buildPipeline(filters: FilterValue[]): PipelineStage[] {
  const pipeline: PipelineStage[] = [];
  for (const filter of filters) {
    const stage = buildMatch(filter);
    if (stage) pipeline.push(stage);
  }
  return pipeline;
}

/**
 * Build pipeline with percentage-based limit support
 */
export async function buildPipelineWithPercent(
  filters: FilterValue[],
  model: Model<unknown>
): Promise<PipelineStage[]> {
  const pipeline: PipelineStage[] = [];

  for (const filter of filters) {
    const stage = buildMatch(filter);
    if (stage) {
      pipeline.push(stage);

      // Handle percentOfResult
      if (filter.percentOfResult) {
        const pct = parseFloat(String(filter.percentOfResult));
        const total =
          (
            await model.aggregate([...pipeline, { $count: "total" }]).exec()
          )?.[0]?.total || 0;
        if (total) {
          const num =
            pct >= 0
              ? Math.ceil((total / 100) * pct)
              : Math.floor((total / 100) * -pct);
          if (pct < 0) pipeline.push({ $skip: total - num });
          pipeline.push({ $limit: num });
        }
      }
    }
  }

  return pipeline;
}

/**
 * Extract filter strings from Request or URL
 */
export function getFiltersFromUrl(
  request: RequestInput,
  tenantValue?: string,
  tenantField = "shopDomain"
): FilterValue[] {
  const url = getUrlFromRequest(request);
  const { searchParams } = new URL(url);
  const filterStrings = searchParams
    .getAll("filter")
    .filter((f) => !f.includes(tenantField));
  const filters = parseFilters(filterStrings);

  // Add tenant filter
  if (tenantValue) {
    filters.unshift({
      field: tenantField,
      type: "string",
      operator: "eq",
      value: tenantValue,
    });
  }

  return filters;
}
