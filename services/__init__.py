"""
SERVICES LAYER CONTRACT

This package contains business logic services and application services.

RULES:
- Implements business logic and use cases
- Coordinates between domain entities and infrastructure
- Contains application services and business rules
- May import from domain and infrastructure layers
- No direct database access (use repositories)

LAYER RESPONSIBILITY:
- Business logic implementation
- Use case orchestration
- Transaction management
- Service layer exceptions

CROSS-LAYER RESTRICTIONS:
- No direct persistence operations
- No UI or presentation logic
- No system-level operations
- Use dependency injection for infrastructure

If you need data access — use persistence layer through interfaces.
"""