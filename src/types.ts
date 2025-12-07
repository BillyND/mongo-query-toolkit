/**
 * @file Types for mongo-query-toolkit
 */

import type { PipelineStage, Model } from "mongoose";

// ============================================================================
// Request Input Type
// ============================================================================

/**
 * Accepts either a Request object or URL string
 */
export type RequestInput = Request | string;

// ============================================================================
// Filter Types
// ============================================================================

export type FilterType = "string" | "date" | "amount" | "array";
export type FilterOperator =
  | "eq"
  | "ne"
  | "has"
  | "nh"
  | "any"
  | "none"
  | "range"
  | "lt"
  | "gt"
  | "before"
  | "after";

export interface BetweenOperatorValue {
  from: string;
  to: string;
}

export interface FilterValue {
  field: string;
  type?: FilterType;
  operator?: FilterOperator;
  value?: string | BetweenOperatorValue;
  percentOfResult?: number | string;
}

// ============================================================================
// Fetch Types
// ============================================================================

export interface FetchListResult<T = Record<string, unknown>> {
  page: number;
  total: number;
  items: T[];
}

export interface MultiModelConfig {
  model: Model<unknown>;
  initialPipeline?: PipelineStage[];
  finalPipeline?: PipelineStage[];
}

// ============================================================================
// Options
// ============================================================================

export interface FetchOptions {
  /** Default: 250 */
  limit?: number;
  /** Default: 'updatedAt' */
  sortField?: string;
  /** Default: 'desc' */
  sortDir?: "asc" | "desc";
  /** Tenant field name, default: 'shopDomain' */
  tenantField?: string;
  /** Tenant value to filter by */
  tenantValue?: string;
}
