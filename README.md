# mongo-query-toolkit

Simple MongoDB aggregation utilities with filtering and pagination.

## Installation

```bash
npm install mongo-query-toolkit
# or
yarn add mongo-query-toolkit
```

## Usage

### Fetch List with Pagination & Filtering

```typescript
import { fetchList } from "mongo-query-toolkit";

// Pass Request object directly (Remix, Next.js, Hono, etc.)
const result = await fetchList(request, MyModel, {
  tenantValue: shopDomain, // Optional: filter by tenant
  limit: 50, // Optional: default 250
});
// => { page: 1, total: 100, items: [...] }

// Or pass URL string
const result = await fetchList(request.url, MyModel, {
  tenantValue: shopDomain,
});
```

### Fetch Single Item

```typescript
import { fetchItem } from "mongo-query-toolkit";

// From URL query param (?id=123)
const item = await fetchItem(request, MyModel);

// Or with explicit ID
const item = await fetchItem(request, MyModel, [], [], "123");
```

### Filter Parsing

```typescript
import {
  parseFilter,
  buildPipeline,
  getFiltersFromUrl,
} from "mongo-query-toolkit";

// Parse single filter string
const filter = parseFilter("status|string|eq|active");

// Get filters from Request or URL
const filters = getFiltersFromUrl(request, shopDomain);

// Build MongoDB pipeline
const pipeline = buildPipeline(filters);
```

### Multi-Model Union Query

```typescript
import { fetchUnifiedList } from "mongo-query-toolkit";

const result = await fetchUnifiedList(
  request,
  [{ model: BoosterModel }, { model: DealModel }],
  { tenantValue: shopDomain }
);
```

## Filter String Format

```
field|type|operator|value
```

**Examples:**

- `status|string|eq|active` - status equals "active"
- `price|amount|gt|100` - price greater than 100
- `createdAt|date|range|2024-01-01~2024-12-31` - date range
- `tags|array|any|a,b,c` - has any of these tags

**Short format (auto-detect type):**

- `status|active` - string contains "active"
- `price|100` - amount equals 100

## License

MIT
