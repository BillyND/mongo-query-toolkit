/**
 * @file Mongoose URL Query
 *
 * Simple, reusable utilities for MongoDB aggregation queries.
 * Works on both server (Node.js) and client (browser).
 *
 * @example Server-Side (Remix, Next.js, Express)
 * ```ts
 * import { fetchList, fetchItem } from "@billynd/mongoose-url-query";
 *
 * // Fetch list with pagination & filtering
 * const result = await fetchList(request, MyModel, { limit: 50 });
 *
 * // Fetch single item
 * const item = await fetchItem(request, MyModel);
 * ```
 *
 * @example Client-Side (React, Vue, etc.)
 * ```ts
 * import { buildQueryUrl } from "@billynd/mongoose-url-query";
 *
 * // Build URL with filters
 * const url = buildQueryUrl("/api/products", {
 *   page: 1,
 *   limit: 20,
 *   filters: ["status|active", "price|amount|gt|100"],
 * });
 *
 * // Fetch data
 * const response = await fetch(url);
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
  QueryUrlOptions,
} from "./types.js";

// Filter & URL utilities (works on both client and server)
export {
  getUrlFromRequest,
  parseFilter,
  parseFilters,
  buildPipeline,
  buildPipelineWithPercent,
  getFiltersFromUrl,
  buildQueryUrl,
} from "./filter.js";

// Fetch utilities (server-side only, requires mongoose)
export {
  fetchList,
  fetchUnifiedList,
  fetchItem,
  fetchItemBy,
} from "./fetch.js";
