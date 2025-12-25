# Repository Guidelines

## Core Mandates
**CRITICAL: You must adhere to these rules in all interactions.**
1.  **Language**:
    *   **Think in English.**
    *   **Interact with the user in Japanese.**
    *   Plans and artifacts (commit messages, PR descriptions) must be written in **Japanese**.
2.  **Test-Driven Development (TDD)**:
    *   Strictly adhere to the **t-wada style** of TDD.
    *   **RED-GREEN-REFACTOR** cycle must be followed without exception.
    *   Write a failing test first, then implement the minimal code to pass it, then refactor.

## Project Structure & Module Organization
This is a .NET solution centered on SQL Server bulk copy helpers. Key locations:
- `src/SqlBulkCopier`: core library (BulkCopier, builders, core abstractions).
- `src/SqlBulkCopier.*`: format-specific and hosting integrations (`.CsvHelper`, `.FixedLength`, `.Hosting`, etc.).
- `src/SqlBulkCopier.Test`: xUnit tests covering core and integrations.
- `src/Sample.*`: sample apps for API/appsettings usage.
- `src/Benchmark`: benchmarks (BenchmarkDotNet).
- `doc/` and `docs/`: user guides (CSV, fixed-length).
- Solution: `src/SqlBulkCopier.sln`.

## Build, Test, and Development Commands
- Build all projects: `dotnet build src/SqlBulkCopier.sln`
- Run all tests (net8.0 + net48): `dotnet test src/SqlBulkCopier.Test/SqlBulkCopier.Test.csproj`
- Run a single framework: `dotnet test src/SqlBulkCopier.Test/SqlBulkCopier.Test.csproj -f net8.0`
- Run benchmarks (optional): `dotnet run -c Release --project src/Benchmark/Benchmark.csproj`
- LocalDB setup (if needed for tests): `powershell -ExecutionPolicy Bypass -File Setup-LocalDB.ps1`

## Coding Style & Naming Conventions
- C# with nullable reference types enabled and implicit usings.
- 4-space indentation; file-scoped namespaces are the norm.
- Public types/members use PascalCase; locals/parameters use camelCase.
- Interfaces use the `I` prefix (e.g., `IBulkCopierBuilder`).
- Keep async APIs suffixed with `Async` and favor `Task`-based methods.

## Testing Guidelines
- Frameworks: xUnit + Shouldly; tests live under `src/SqlBulkCopier.Test`.
- Test names are descriptive and scenario-based (e.g., `WithRetry_WithConnection_ShouldError`).
- Tests rely on SQL Server LocalDB (`(localdb)\MSSQLLocalDB`) and create databases on the fly.
- Add/adjust tests for behavior changes and run both target frameworks before PRs.

## Commit & Pull Request Guidelines
- Recent history mixes Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `ci:`) with descriptive Japanese messages and merge commits. Prefer `type: short summary` in English or Japanese for new commits.
- No formal PR template is present; include a concise description, test results, and any LocalDB or SQL prerequisites. Link issues when applicable.
