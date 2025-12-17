# ETL Router Web Dashboard

A SvelteKit web application for monitoring and managing the ETL Router cluster.

## Features

- **Dashboard** - Overview of cluster status, services, and pipelines
- **Services** - View and manage registered services with health status
- **Pipelines** - Visualize pipeline DAGs and manage pipeline configurations
- **Cluster** - Monitor Raft cluster state and node information
- **Metrics** - Real-time throughput estimation, consumer lag tracking, and offset monitoring
- **Admin** - User management with role-based access control (when auth is enabled)

## Tech Stack

- [SvelteKit](https://kit.svelte.dev/) - Full-stack framework
- [Svelte 5](https://svelte.dev/) - UI framework with runes
- [URQL](https://formidable.com/open-source/urql/) - GraphQL client
- [Tailwind CSS](https://tailwindcss.com/) - Styling
- [Prisma](https://www.prisma.io/) - Database ORM
- [better-auth](https://www.better-auth.com/) - Authentication (optional)
- [Playwright](https://playwright.dev/) - E2E testing
- [@xyflow/svelte](https://svelteflow.dev/) - Pipeline DAG visualization

## Prerequisites

- Node.js 20+
- ETL Router GraphQL API running at `http://localhost:8080/graphql`

## Getting Started

1. **Install dependencies**

   ```bash
   npm install
   ```

2. **Set up environment**

   ```bash
   cp .env.example .env
   ```

3. **Initialize database**

   ```bash
   npm run db:generate
   npm run db:push
   ```

4. **Start development server**

   ```bash
   npm run dev
   ```

   Open [http://localhost:5173](http://localhost:5173)

## Scripts

| Script | Description |
|--------|-------------|
| `npm run dev` | Start development server |
| `npm run build` | Build for production |
| `npm run preview` | Preview production build |
| `npm run check` | Run type checking |
| `npm run db:generate` | Generate Prisma client |
| `npm run db:push` | Push schema to database |
| `npm run db:migrate` | Run database migrations |
| `npm run db:studio` | Open Prisma Studio |
| `npm run test:e2e` | Run Playwright tests |
| `npm run test:e2e:ui` | Run tests with Playwright UI |
| `npm run test:e2e:headed` | Run tests in headed browser |

## Authentication

Authentication is **disabled by default**. All pages are accessible without login.

### Enabling Authentication

To enable authentication, insert a settings record in the database:

```sql
INSERT INTO settings (id, authEnabled, createdAt, updatedAt)
VALUES ('default', true, datetime('now'), datetime('now'));
```

Or using Prisma Studio:

```bash
npm run db:studio
```

Then navigate to the `settings` table and create a record with `authEnabled: true`.

### OAuth Setup

When auth is enabled, configure OAuth providers in `.env`:

```env
# Google OAuth (https://console.cloud.google.com/apis/credentials)
GOOGLE_CLIENT_ID=your-client-id
GOOGLE_CLIENT_SECRET=your-client-secret

# GitHub OAuth (https://github.com/settings/developers)
GITHUB_CLIENT_ID=your-client-id
GITHUB_CLIENT_SECRET=your-client-secret
```

Callback URLs:
- Google: `http://localhost:5173/api/auth/callback/google`
- GitHub: `http://localhost:5173/api/auth/callback/github`

### Roles

| Role | Permissions |
|------|-------------|
| `USER` | View dashboard, services, pipelines, cluster, metrics |
| `ADMIN` | All USER permissions + user management |
| `SUPER_ADMIN` | All ADMIN permissions + assign SUPER_ADMIN role |

Set `SUPER_ADMIN_EMAIL` in `.env` to auto-promote the first matching user on sign-in.

## Database

### SQLite (Default)

SQLite is used by default for development:

```env
DATABASE_URL="file:./prisma/dev.db"
```

### PostgreSQL (Production)

For production, use PostgreSQL:

1. Copy the PostgreSQL schema:
   ```bash
   cp prisma/schema.postgresql.prisma prisma/schema.prisma
   ```

2. Update `.env`:
   ```env
   DATABASE_URL="postgresql://user:password@localhost:5432/etl_router"
   ```

3. Regenerate and push:
   ```bash
   npm run db:generate
   npm run db:push
   ```

## Project Structure

```
web/
├── e2e/playwright/          # E2E tests
├── prisma/
│   ├── schema.prisma        # Database schema (SQLite)
│   └── schema.postgresql.prisma
├── src/
│   ├── lib/
│   │   ├── components/      # Svelte components
│   │   │   └── ui/          # UI primitives
│   │   ├── graphql/         # GraphQL client and queries
│   │   └── server/          # Server-side utilities
│   └── routes/              # SvelteKit routes
│       ├── admin/           # Admin pages
│       ├── auth/            # Auth pages
│       ├── cluster/         # Cluster status
│       ├── metrics/         # Metrics dashboard
│       ├── pipelines/       # Pipeline management
│       └── services/        # Service management
├── playwright.config.ts     # Playwright configuration
└── tailwind.config.js       # Tailwind configuration
```

## E2E Testing

Tests are located in `e2e/playwright/`.

```bash
# Run all tests
npm run test:e2e

# Run with UI
npm run test:e2e:ui

# Run specific test file
npx playwright test navigation.spec.ts

# View HTML report
npx playwright show-report
```

### CI/CD

GitHub Actions workflow (`.github/workflows/web-e2e.yml`) runs tests on push/PR and uploads:
- `playwright-report` - Interactive HTML report with screenshots
- `test-screenshots` - Raw test results

## GraphQL API

The dashboard connects to the ETL Router GraphQL API. Key queries:

- `clusterStatus` - Raft cluster state
- `services` - Registered services
- `pipelines` - Pipeline configurations
- `consumerGroups` - Consumer group state
- `sourceOffsets` - Source partition offsets
- `groupOffsets` - Consumer group offsets
- `watermarks` - Event time watermarks

## License

Licensed under either of Apache License, Version 2.0 or MIT license, at your option.

See [LICENSE-APACHE](../LICENSE-APACHE) and [LICENSE-MIT](../LICENSE-MIT) in the repository root.
