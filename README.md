# @billynd/mongoose-url-query

[![npm version](https://img.shields.io/npm/v/@billynd/mongoose-url-query.svg)](https://www.npmjs.com/package/@billynd/mongoose-url-query)
[![npm downloads](https://img.shields.io/npm/dm/@billynd/mongoose-url-query.svg)](https://www.npmjs.com/package/@billynd/mongoose-url-query)
[![license](https://img.shields.io/npm/l/@billynd/mongoose-url-query.svg)](https://github.com/BillyND/mongoose-url-query/blob/main/LICENSE)

A simple, URL-based Mongoose query toolkit for Node.js. Parse filters from URL query strings, build aggregation pipelines, and fetch paginated data with ease.

**Perfect for:** REST APIs, Remix, Next.js, Hono, Express, and any framework that uses Request objects.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Filter System](#filter-system)
- [Pipeline Customization](#pipeline-customization)
- [Client-Side Usage](#client-side-usage)
- [Response Format](#response-format)
- [Changelog](#changelog)

---

## Installation

```bash
npm install @billynd/mongoose-url-query
# or
yarn add @billynd/mongoose-url-query
```

**Peer Dependency:** Requires `mongoose >= 6.0.0`

---

## Quick Start

```typescript
import { fetchList, fetchItem } from "@billynd/mongoose-url-query";
import { ProductModel } from "./models";

// Fetch paginated list with filters from URL
// URL: /api/products?filter=status|active&filter=price|amount|gt|100&page=2&limit=20
const result = await fetchList(request, ProductModel);
// => { page: 2, total: 150, items: [...20 products...] }

// With multi-tenant support
const result = await fetchList(request, ProductModel, {
  tenantField: "organizationId",
  tenantValue: currentOrg.id,
});
// Automatically adds filter: { organizationId: currentOrg.id }

// Fetch single item by ID
// URL: /api/products?id=abc123
const item = await fetchItem(request, ProductModel);
// => { _id: "abc123", name: "Product", ... } or null
```

---

## Core Concepts

### 1. Request Input

All functions accept either a **Request object** or **URL string**:

```typescript
// ✅ Pass Request directly (Remix, Next.js, Hono)
await fetchList(request, Model);

// ✅ Pass URL string
await fetchList(request.url, Model);
await fetchList("http://localhost/api?filter=status|active", Model);
```

### 2. Pipeline Execution Order

Understanding the order helps you customize queries effectively:

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. initialPipeline    →  Runs FIRST (setup, computed fields)       │
│  2. URL Filters        →  Filters from ?filter=... params           │
│  3. Count Total        →  Get total count for pagination            │
│  4. Sort               →  Sort results                              │
│  5. finalPipeline      →  Runs AFTER sort (lookups, projections)    │
│  6. Pagination         →  $skip and $limit applied LAST             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## API Reference

### `fetchList(request, model, options?, initialPipeline?, finalPipeline?)`

Fetch a paginated list with filtering and sorting.

**Parameters:**

| Parameter         | Type                | Description                           |
| ----------------- | ------------------- | ------------------------------------- |
| `request`         | `Request \| string` | Request object or URL string          |
| `model`           | `Model`             | Mongoose model                        |
| `options`         | `FetchOptions`      | Configuration options                 |
| `initialPipeline` | `PipelineStage[]`   | Pipeline stages to run BEFORE filters |
| `finalPipeline`   | `PipelineStage[]`   | Pipeline stages to run AFTER sort     |

**Options:**

| Option        | Type              | Default       | Description                                      |
| ------------- | ----------------- | ------------- | ------------------------------------------------ |
| `limit`       | `number`          | `250`         | Max items per page                               |
| `sortField`   | `string`          | `"updatedAt"` | Default sort field                               |
| `sortDir`     | `"asc" \| "desc"` | `"desc"`      | Default sort direction                           |
| `tenantField` | `string`          | -             | Field name for tenant/scope (e.g. "orgId")       |
| `tenantValue` | `string`          | -             | Value to filter by (requires tenantField to set) |

**Example:**

```typescript
// Basic usage (no multi-tenant)
const result = await fetchList(request, ProductModel, {
  limit: 50,
  sortField: "createdAt",
});

// With multi-tenant filtering
const result = await fetchList(request, ProductModel, {
  tenantField: "organizationId",
  tenantValue: currentOrg.id,
  limit: 50,
});
```

---

### `fetchItem(request, model, initialPipeline?, finalPipeline?, id?)`

Fetch a single item by ID.

**Parameters:**

| Parameter         | Type                | Description                                      |
| ----------------- | ------------------- | ------------------------------------------------ |
| `request`         | `Request \| string` | Request object or URL string                     |
| `model`           | `Model`             | Mongoose model                                   |
| `initialPipeline` | `PipelineStage[]`   | Pipeline stages before $match                    |
| `finalPipeline`   | `PipelineStage[]`   | Pipeline stages after $match                     |
| `id`              | `string \| number`  | Explicit ID (optional, defaults to `?id=` param) |

**ID Matching:** Automatically matches against `id`, `_id` (as string, number, or ObjectId).

**Example:**

```typescript
// From URL: /api/products?id=abc123
const item = await fetchItem(request, ProductModel);

// With explicit ID
const item = await fetchItem(request, ProductModel, [], [], "abc123");
```

---

### `fetchItemBy(model, field, value, pipeline?)`

Fetch a single item by any field value.

```typescript
// Find by email
const user = await fetchItemBy(UserModel, "email", "john@example.com");

// Find by slug with lookup
const post = await fetchItemBy(PostModel, "slug", "my-post", [
  {
    $lookup: {
      from: "users",
      localField: "authorId",
      foreignField: "_id",
      as: "author",
    },
  },
]);
```

---

### `fetchUnifiedList(request, models, options?)`

Fetch from multiple collections using `$unionWith`, useful for combining different document types.

```typescript
const result = await fetchUnifiedList(
  request,
  [
    { model: PostModel, initialPipeline: [...] },
    { model: CommentModel, initialPipeline: [...] },
  ],
  {
    tenantField: "workspaceId",
    tenantValue: workspace.id,
  }
);
// Each item will have `_sourceType` field indicating the source collection
```

---

## Filter System

### Filter String Format

Filters are passed via URL query parameters: `?filter=field|type|operator|value`

**Full Format:** `field|type|operator|value`

```
?filter=status|string|eq|active
?filter=price|amount|gt|100
?filter=createdAt|date|range|2024-01-01~2024-12-31
```

**Short Format:** `field|value` (auto-detects type and operator)

```
?filter=status|active        → string contains "active"
?filter=price|100            → amount equals 100
?filter=id|abc123            → id equals "abc123"
?filter=tags|a,b,c           → array has any of [a,b,c]
?filter=date|2024-01-01~2024-12-31  → date range
```

### Short Format Auto-Detection Rules

When using short format, the library auto-detects type and operator based on these rules:

**Type Detection:**

| Value Pattern                           | Detected Type | Exception                      |
| --------------------------------------- | ------------- | ------------------------------ |
| `2024-01-01` or `2024-01-01~2024-12-31` | `date`        | -                              |
| Only digits: `100`, `99.99`             | `amount`      | Unless field is `name` or `id` |
| Contains comma: `a,b,c`                 | `array`       | -                              |
| Everything else                         | `string`      | -                              |

**Operator Detection:**

| Condition              | Detected Operator  |
| ---------------------- | ------------------ |
| Field is `id` or `_id` | `eq` (exact match) |
| Value contains `~`     | `range`            |
| Value contains `,`     | `any`              |
| Default                | `has` (contains)   |

**Examples:**

```
?filter=name|123       → type: string, operator: has (name is forced string)
?filter=price|123      → type: amount, operator: has
?filter=id|abc123      → type: string, operator: eq (id uses exact match)
?filter=status|active  → type: string, operator: has
```

> **Tip:** Use **Full Format** (`field|type|operator|value`) when you need explicit control over type and operator.

### Supported Types

| Type     | Description        | Example Values                        |
| -------- | ------------------ | ------------------------------------- |
| `string` | Text fields        | `"active"`, `"john"`                  |
| `amount` | Numbers            | `100`, `99.99`                        |
| `date`   | Dates (ISO format) | `2024-01-01`, `2024-01-01~2024-12-31` |
| `array`  | Array fields       | `a,b,c` (comma-separated)             |

### Supported Operators

| Operator | Name         | Types                 | Example                               | MongoDB Equivalent                   |
| -------- | ------------ | --------------------- | ------------------------------------- | ------------------------------------ |
| `eq`     | Equals       | all                   | `status\|string\|eq\|active`          | `{ status: "active" }`               |
| `ne`     | Not Equals   | string, amount, array | `status\|string\|ne\|deleted`         | `{ status: { $ne: "deleted" } }`     |
| `has`    | Contains     | string                | `name\|string\|has\|john`             | `{ name: /john/i }`                  |
| `nh`     | Not Contains | string                | `name\|string\|nh\|test`              | `{ name: { $not: /test/i } }`        |
| `any`    | In Array     | string, array         | `status\|array\|any\|a,b,c`           | `{ status: { $in: ["a","b","c"] } }` |
| `none`   | Not In Array | string, array         | `status\|array\|none\|x,y`            | `{ status: { $nin: ["x","y"] } }`    |
| `range`  | Between      | date, amount          | `price\|amount\|range\|10~100`        | `{ price: { $gte: 10, $lte: 100 } }` |
| `lt`     | Less Than    | amount                | `price\|amount\|lt\|100`              | `{ price: { $lt: 100 } }`            |
| `gt`     | Greater Than | amount                | `price\|amount\|gt\|50`               | `{ price: { $gt: 50 } }`             |
| `before` | Before Date  | date                  | `createdAt\|date\|before\|2024-01-01` | `{ createdAt: { $lt: Date } }`       |
| `after`  | After Date   | date                  | `createdAt\|date\|after\|2024-01-01`  | `{ createdAt: { $gt: Date } }`       |

### Percentage-Based Results

You can limit results to a **percentage** of the filtered data by adding a 5th parameter:

**Format:** `field|type|operator|value|percentOfResult`

| Value | Behavior                     | Example (100 records total) |
| ----- | ---------------------------- | --------------------------- |
| `10`  | Get **first 10%** of results | Returns first 10 records    |
| `-10` | Get **last 10%** of results  | Skips 90, returns last 10   |

**Examples:**

```
# Get top 10% of products with price > 0
?filter=price|amount|gt|0|10

# Get bottom 20% of users by score
?filter=score|amount|gt|0|-20

# Get first 5% of active orders
?filter=status|string|eq|active|5
```

**Use Cases:**

- **Top performers:** Get top 10% highest-value customers
- **Bottom analysis:** Identify bottom 20% lowest-performing products
- **Sampling:** Quick preview of a percentage of large datasets

> **Note:** Percentage is calculated AFTER filters are applied. If filter returns 1000 items and you use `|10`, you get 100 items.

---

## Pipeline Customization

### initialPipeline

**What:** MongoDB aggregation stages that run **BEFORE** URL filters are applied.

**Use Cases:**

- Create computed/virtual fields for filtering
- Add base $match conditions that always apply
- $lookup to join related data before filtering

**Example: Create searchable field**

```typescript
const result = await fetchList(request, ProductModel, { limit: 50 }, [
  // Create a combined search field from multiple fields
  {
    $addFields: {
      searchText: {
        $concat: [
          { $toString: "$_id" },
          " ",
          { $ifNull: ["$title", ""] },
          " ",
          { $ifNull: ["$name", ""] },
          " ",
          { $ifNull: ["$sku", ""] },
        ],
      },
    },
  },
  // Always exclude deleted items
  { $match: { deletedAt: null, isActive: true } },
]);
// Client can now filter: ?filter=searchText|string|has|keyword
```

### finalPipeline

**What:** MongoDB aggregation stages that run **AFTER** sorting but **BEFORE** pagination.

**Use Cases:**

- $lookup to join related data (runs on sorted data, before limiting)
- $project to shape the final output
- $unwind to flatten arrays

**Example: Add related data**

```typescript
const result = await fetchList(
  request,
  OrderModel,
  { limit: 50 },
  [], // no initialPipeline
  [
    // Lookup customer info
    {
      $lookup: {
        from: "customers",
        localField: "customerId",
        foreignField: "_id",
        as: "customer",
      },
    },
    { $unwind: { path: "$customer", preserveNullAndEmptyArrays: true } },
    // Shape output
    {
      $project: {
        _id: 1,
        orderNumber: 1,
        total: 1,
        status: 1,
        customerName: "$customer.name",
        customerEmail: "$customer.email",
      },
    },
  ]
);
```

### Combined Example

```typescript
const result = await fetchList(
  request,
  ProductModel,
  {
    tenantField: "storeId",
    tenantValue: currentStore.id,
    limit: 20,
    sortField: "createdAt",
  },
  // initialPipeline: runs FIRST
  [
    { $addFields: { searchText: { $concat: ["$title", " ", "$sku"] } } },
    { $match: { isActive: true } },
  ],
  // finalPipeline: runs AFTER sort, BEFORE pagination
  [
    {
      $lookup: {
        from: "categories",
        localField: "categoryId",
        foreignField: "_id",
        as: "category",
      },
    },
    { $unwind: { path: "$category", preserveNullAndEmptyArrays: true } },
  ]
);
```

---

## Client-Side Usage

### `buildQueryUrl(baseUrl, options)`

Build a query URL with filters, pagination, and sorting. **Works in both browser and Node.js.**

```typescript
import { buildQueryUrl } from "@billynd/mongoose-url-query";

const url = buildQueryUrl("/api/products", {
  page: 1,
  limit: 20,
  sort: "price|desc",
  filters: [
    "status|string|eq|active",
    "price|amount|range|100~500",
    "category|array|any|electronics,books",
  ],
});
// => "/api/products?page=1&limit=20&sort=price|desc&filter=status|string|eq|active&filter=..."
```

**Options:**

| Option      | Type       | Description                             |
| ----------- | ---------- | --------------------------------------- |
| `page`      | `number`   | Page number                             |
| `limit`     | `number`   | Items per page                          |
| `sort`      | `string`   | Sort: `"field\|asc"` or `"field\|desc"` |
| `filters`   | `string[]` | Array of filter strings                 |
| `id`        | `string`   | Item ID (for fetchItem)                 |
| `export`    | `boolean`  | Skip pagination                         |
| `countOnly` | `boolean`  | Return only count                       |

### React Example

```tsx
import { buildQueryUrl } from "@billynd/mongoose-url-query";

function ProductList() {
  const [products, setProducts] = useState([]);
  const [filters, setFilters] = useState<string[]>([]);
  const [page, setPage] = useState(1);

  useEffect(() => {
    const url = buildQueryUrl("/api/products", {
      page,
      limit: 20,
      filters,
    });

    fetch(url)
      .then((res) => res.json())
      .then((data) => setProducts(data.items));
  }, [page, filters]);

  return (
    <div>
      <button onClick={() => setFilters([...filters, "status|string|eq|active"])}>
        Active Only
      </button>
      <button onClick={() => setFilters([...filters, "price|amount|gt|100"])}>
        Price > $100
      </button>
      {/* Render products */}
    </div>
  );
}
```

### Remix Example

```tsx
// app/routes/products.tsx
import { json, type LoaderFunctionArgs } from "@remix-run/node";
import { useLoaderData, useSearchParams } from "@remix-run/react";
import { fetchList } from "@billynd/mongoose-url-query";

export async function loader({ request }: LoaderFunctionArgs) {
  const result = await fetchList(request, ProductModel, { limit: 20 });
  return json(result);
}

export default function Products() {
  const { items, total, page } = useLoaderData<typeof loader>();
  const [searchParams, setSearchParams] = useSearchParams();

  const addFilter = (filter: string) => {
    const params = new URLSearchParams(searchParams);
    params.append("filter", filter);
    setSearchParams(params);
  };

  return (
    <div>
      <p>Total: {total} items</p>
      <button onClick={() => addFilter("status|active")}>Active</button>
      {items.map((item) => (
        <div key={item._id}>{item.name}</div>
      ))}
    </div>
  );
}
```

---

## Response Format

### fetchList / fetchUnifiedList

```typescript
interface FetchListResult<T> {
  page: number; // Current page number (1-indexed)
  limit: number; // Items per page
  total: number; // Total matching documents
  totalPages: number; // Total number of pages
  hasNextPage: boolean; // Whether there is a next page
  hasPrevPage: boolean; // Whether there is a previous page
  nextPage: number | null; // Next page number (null if none)
  prevPage: number | null; // Previous page number (null if none)
  items: T[]; // Array of documents for current page
}
```

**Example Response:**

```json
{
  "page": 2,
  "limit": 20,
  "total": 150,
  "totalPages": 8,
  "hasNextPage": true,
  "hasPrevPage": true,
  "nextPage": 3,
  "prevPage": 1,
  "items": [
    { "_id": "...", "name": "Product 1" },
    { "_id": "...", "name": "Product 2" }
  ]
}
```

**Pagination Helpers for Client:**

```typescript
// Easy to build pagination UI
const { page, totalPages, hasNextPage, hasPrevPage, nextPage, prevPage } = result;

// Example: Pagination component
<button disabled={!hasPrevPage} onClick={() => goToPage(prevPage)}>Prev</button>
<span>Page {page} of {totalPages}</span>
<button disabled={!hasNextPage} onClick={() => goToPage(nextPage)}>Next</button>
```

### fetchItem / fetchItemBy

Returns the document or `null` if not found.

```typescript
// Found
{ _id: "abc123", name: "Product", price: 99.99, ... }

// Not found
null
```

---

## URL Query Parameters

| Parameter         | Description                         | Example                                                |
| ----------------- | ----------------------------------- | ------------------------------------------------------ |
| `page`            | Page number (default: 1)            | `?page=2`                                              |
| `limit`           | Items per page (max: options.limit) | `?limit=20`                                            |
| `sort`            | Sort field and direction            | `?sort=price\|desc` or `?sort=createdAt\|asc`          |
| `filter`          | Filter string (repeatable)          | `?filter=status\|active&filter=price\|amount\|gt\|100` |
| `id`              | Item ID for fetchItem               | `?id=abc123`                                           |
| `export`          | Skip pagination (return all)        | `?export=true`                                         |
| `countResultOnly` | Return only count, no items         | `?countResultOnly=true`                                |

---

## TypeScript Support

Full TypeScript support with generics:

```typescript
interface Product {
  _id: string;
  name: string;
  price: number;
}

const result = await fetchList<Product>(request, ProductModel);
// result.items is Product[]

const item = await fetchItem<Product>(request, ProductModel);
// item is Product | null
```

---

## Changelog

See [GitHub Releases](https://github.com/BillyND/mongoose-url-query/releases) for version history and release notes.

---

## License

MIT
