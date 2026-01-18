# Contributing to Backstage

Thank you for your interest in contributing to Backstage! We welcome contributions from the community.

## Getting Started

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally.
3.  **Install dependencies**:
    - **TypeScript**: `bun install` in `packages/backstage-ts`
    - **Go**: `go mod download` in `packages/backstage-go`

## Development Guidelines

### TypeScript SDK

- **Runtime**: Use [Bun](https://bun.sh) (v1.3+).
- **Testing**: Run `bun test` to execute the test suite.
- **Code Style**: Ensure your code follows the existing style conventions.

### Go SDK

- **Version**: Go 1.21+.
- **Testing**: Run `go test ./...` to execute the test suite.
- **Formatting**: Run `go fmt ./...` before committing.

## Submitting Changes

1.  Create a new branch for your feature or fix: `git checkout -b feature/my-new-feature`
2.  Commit your changes with clear, descriptive messages.
3.  Push your branch to your fork.
4.  Open a **Pull Request** against the `main` branch.

## Issue Reporting

If you find a bug or have a feature request, please open an issue on GitHub. Provide as much detail as possible, including reproduction steps for bugs.

## License

By contributing, you agree that your contributions will be licensed under its [MIT License](LICENSE).
