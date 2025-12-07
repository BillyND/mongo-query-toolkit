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
  /** Current page number (1-indexed) */
  page: number;
  /** Items per page */
  limit: number;
  /** Total matching documents */
  total: number;
  /** Total number of pages */
  totalPages: number;
  /** Whether there is a next page */
  hasNextPage: boolean;
  /** Whether there is a previous page */
  hasPrevPage: boolean;
  /** Next page number (null if no next page) */
  nextPage: number | null;
  /** Previous page number (null if no previous page) */
  prevPage: number | null;
  /** Array of documents for current page */
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
  /** Max items per page. Default: 250 */
  limit?: number;
  /** Default sort field. Default: 'updatedAt' */
  sortField?: string;
  /** Default sort direction. Default: 'desc' */
  sortDir?: "asc" | "desc";
  /** Field name for tenant/scope filtering (required if tenantValue is set) */
  tenantField?: string;
  /** Value to filter by tenant/scope field */
  tenantValue?: string;
}

// ============================================================================
// Client-Side URL Builder
// ============================================================================

export interface QueryUrlOptions {
  /** Page number */
  page?: number;
  /** Items per page */
  limit?: number;
  /** Sort field and direction: "field|asc" or "field|desc" */
  sort?: string;
  /** Filter strings in format: "field|type|operator|value" or "field|value" */
  filters?: string[];
  /** Item ID for fetchItem */
  id?: string;
  /** Skip pagination and return all results */
  export?: boolean;
  /** Return only count, no items */
  countOnly?: boolean;
}
