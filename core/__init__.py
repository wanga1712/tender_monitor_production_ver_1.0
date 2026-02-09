"""
CORE LAYER CONTRACT

This package contains core application components and abstractions.

RULES:
- Contains fundamental building blocks for all layers
- Defines base classes, interfaces, and abstractions
- No business logic implementation
- No infrastructure dependencies
- Pure abstractions and contracts only

LAYER RESPONSIBILITY:
- ApplicationError hierarchy and base exceptions
- Database connection abstractions
- Service interfaces and contracts
- Domain entity base classes

CROSS-LAYER RESTRICTIONS:
- No imports from domain, infrastructure, persistence, or orchestration
- No filesystem, OS, or network access
- Only core abstractions and interfaces

If you need concrete implementations — you are in the wrong layer.
"""