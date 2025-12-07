/**
 * @file MongoDB Query Toolkit
 *
 * Simple, reusable utilities for MongoDB aggregation queries.
 *
 * @example Basic Usage
 * ```ts
 * import { fetchList, fetchItem } from "mongo-query-toolkit";
 *
 * // Fetch list with pagination & filtering (accepts Request or URL string)
 * const result = await fetchList(request, MyModel, {
 *   tenantValue: shopDomain,
 *   limit: 50,
 * });
 *
 * // Fetch single item
 * const item = await fetchItem(request, MyModel);
 * ```
 *
 * @example Filter Parsing
 * ```ts
 * import { parseFilter, buildPipeline, getFiltersFromUrl } from "mongo-query-toolkit";
 *
 * // Parse filter string
 * const filter = parseFilter("status|string|eq|active");
 *
 * // Get filters from Request or URL
 * const filters = getFiltersFromUrl(request, shopDomain);
 *
 * // Build MongoDB pipeline
 * const pipeline = buildPipeline(filters);
 * ```
 */

// Types
export type {
  RequestInput,
  FilterType,
  FilterOperator,
  FilterValue,
  BetweenOperatorValue,
  FetchListResult,
  FetchOptions,
  MultiModelConfig,
} from "./types.js";

// Filter utilities
export {
  getUrlFromRequest,
  parseFilter,
  parseFilters,
  buildPipeline,
  buildPipelineWithPercent,
  getFiltersFromUrl,
} from "./filter.js";

// Fetch utilities
export { fetchList, fetchUnifiedList, fetchItem, fetchItemBy } from "./fetch.js";
