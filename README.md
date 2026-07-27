# auth-svc

The **Auth Service** is the Identity bounded context responsible for **user registration**, **login**, **JWT issuance**, **token refresh**, and **OAuth2 social login** (Google, GitHub) in the Harmony platform. It is written in **.NET 8** (ASP.NET Core) and follows **Clean Architecture** (Robert C. Martin) combined with **DDD** layering and the **CQRS** pattern via **MediatR**.

---

## Table of contents

- [Overview](#overview)
- [Bounded context](#bounded-context)
- [Solution structure](#solution-structure)
- [Architecture & dependency rule](#architecture--dependency-rule)
- [Domain layer](#domain-layer)
- [Application layer](#application-layer)
- [Infrastructure layer](#infrastructure-layer)
- [API layer](#api-layer)
- [Authentication flows](#authentication-flows)
- [JWT strategy](#jwt-strategy)
- [Domain events](#domain-events)
- [Database](#database)
- [Local development](#local-development)
- [Testing strategy](#testing-strategy)
- [Environment variables](#environment-variables)
- [Related services](#related-services)

---

## Overview

| Property | Value |
|---|---|
| Runtime | .NET 8 / ASP.NET Core |
| Architecture | Clean Architecture + DDD + CQRS (MediatR) |
| Database | PostgreSQL (EF Core) |
| Cache | Redis (session + rate limit) |
| Messaging | Apache Kafka (outbox pattern) |
| Token format | JWT RS256 (access) + opaque (refresh) |
| HTTP port | `8084` |
| gRPC port | `9084` |

---

## Bounded context

`auth-svc` owns **who a user is from an identity perspective** — credentials, sessions, and token lifecycle. It does not own profile data (that belongs to `user-svc`). Other services trust the JWT issued here.

```
auth-svc ──► identity.user.registered ──► user-svc (creates profile)
         ──► identity.user.registered ──► search-svc (index user)

community-svc ──► gRPC ValidateToken ──► auth-svc (internal token check)
ws-gateway    ──► gRPC ValidateToken ──► auth-svc
```

> **Rule:** auth-svc is the only service that issues, validates, or revokes tokens. Other services verify signatures using the public key from the JWKS endpoint — they never call auth-svc per-request.

---

## Solution structure

```
Harmony.AuthService/
├── Harmony.AuthService.sln
│
├── src/
│   ├── Harmony.AuthService.Domain/          ← Class library — zero external dependencies
│   ├── Harmony.AuthService.Application/     ← Class library — refs Domain only
│   ├── Harmony.AuthService.Infrastructure/  ← Class library — refs Application + Domain
│   └── Harmony.AuthService.API/             ← ASP.NET Core Web API — refs all projects
│
└── tests/
    ├── Harmony.AuthService.Domain.Tests/
    ├── Harmony.AuthService.Application.Tests/
    └── Harmony.AuthService.Integration.Tests/
```

### Dependency rule

```
API → Infrastructure → Application → Domain
 ↑                                      ↑
 └──────────── only inward ─────────────┘
```

The **Domain** project has no `<PackageReference>` to any NuGet package — only BCL types. This is enforced by CI (a build step that verifies `Domain.csproj` has no external references).

---

## Architecture & dependency rule

### Project responsibilities

| Project | Type | Allowed imports | Purpose |
|---|---|---|---|
| `Domain` | Class library | BCL only | Aggregates, value objects, domain events, repository interfaces |
| `Application` | Class library | `Domain` only | MediatR commands/queries, port interfaces, DTOs, pipeline behaviors |
| `Infrastructure` | Class library | `Application` + `Domain` + NuGet | EF Core, JWT, BCrypt, Kafka, OAuth HTTP clients — all adapters |
| `API` | ASP.NET Core Web API | All projects | Controllers, middleware, `Program.cs` DI wiring, `appsettings.json` |

### What DI wiring looks like (`Program.cs`)

```csharp
// Application layer registers itself
builder.Services.AddApplication();   // MediatR + FluentValidation + behaviors

// Infrastructure registers adapters against Application ports
builder.Services.AddInfrastructure(builder.Configuration);
// Inside AddInfrastructure:
//   services.AddScoped<IUserRepository, UserEfRepository>();
//   services.AddScoped<ITokenService, JwtTokenService>();
//   services.AddScoped<IPasswordHasher, BCryptPasswordHasher>();
//   services.AddScoped<IEventPublisher, KafkaOutboxPublisher>();
//   services.AddScoped<IOAuthProvider, GoogleOAuthClient>("google");
//   services.AddScoped<IOAuthProvider, GitHubOAuthClient>("github");
```

---

## Domain layer

`Harmony.AuthService.Domain/`

The domain is a **pure C# class library**. No EF Core, no ASP.NET, no NuGet references whatsoever — only the .NET BCL.

### Folder structure

```
Domain/
├── Entities/
│   ├── User.cs              ← Aggregate root
│   └── RefreshToken.cs      ← Entity — owned by User
├── ValueObjects/
│   ├── Email.cs             ← Validated email address
│   ├── HashedPassword.cs    ← Opaque wrapper — never exposes raw hash
│   └── OAuthProvider.cs     ← Value object: Provider + ProviderUserId
├── Events/
│   ├── IDomainEvent.cs      ← Marker interface
│   ├── UserRegisteredEvent.cs
│   ├── UserLoggedInEvent.cs
│   └── UserSessionRevokedEvent.cs
├── Repositories/
│   ├── IUserRepository.cs   ← Domain port
│   └── IRefreshTokenRepository.cs
└── Exceptions/
    ├── InvalidEmailException.cs
    ├── DuplicateEmailException.cs
    ├── InvalidCredentialsException.cs
    └── TokenExpiredException.cs
```

### `User` aggregate root

```csharp
public sealed class User
{
    private readonly List<IDomainEvent> _domainEvents = new();
    private readonly List<RefreshToken> _refreshTokens = new();

    public Guid Id { get; private set; }
    public Email Email { get; private set; }
    public HashedPassword Password { get; private set; }
    public OAuthProvider? OAuthProvider { get; private set; }
    public bool IsEmailVerified { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public IReadOnlyList<IDomainEvent> DomainEvents => _domainEvents.AsReadOnly();

    private User() { }   // EF Core + reconstitution

    // ── Factory ──────────────────────────────────────────────────────────────
    public static User Create(Email email, HashedPassword password)
    {
        var user = new User
        {
            Id        = Guid.NewGuid(),
            Email     = email,
            Password  = password,
            CreatedAt = DateTimeOffset.UtcNow
        };
        user._domainEvents.Add(new UserRegisteredEvent(user.Id, email.Value));
        return user;
    }

    public static User CreateFromOAuth(Email email, OAuthProvider provider)
    {
        var user = new User
        {
            Id            = Guid.NewGuid(),
            Email         = email,
            OAuthProvider = provider,
            IsEmailVerified = true,   // OAuth providers verify email
            CreatedAt     = DateTimeOffset.UtcNow
        };
        user._domainEvents.Add(new UserRegisteredEvent(user.Id, email.Value));
        return user;
    }

    // ── Behaviour ─────────────────────────────────────────────────────────────
    public RefreshToken IssueRefreshToken(string token, DateTimeOffset expiresAt)
    {
        var rt = new RefreshToken(Id, token, expiresAt);
        _refreshTokens.Add(rt);
        return rt;
    }

    public void RevokeAllSessions()
    {
        foreach (var rt in _refreshTokens) rt.Revoke();
        _domainEvents.Add(new UserSessionRevokedEvent(Id));
    }

    public IReadOnlyList<IDomainEvent> PullDomainEvents()
    {
        var events = _domainEvents.ToList();
        _domainEvents.Clear();
        return events;
    }
}
```

### Value objects

```csharp
// Email — enforces RFC 5321 format
public record Email
{
    public string Value { get; }
    public Email(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || !IsValidEmail(value))
            throw new InvalidEmailException(value);
        Value = value.ToLowerInvariant();
    }
}

// HashedPassword — opaque; never exposes the raw hash string
public record HashedPassword
{
    private readonly string _hash;
    private HashedPassword(string hash) => _hash = hash;

    public static HashedPassword FromHash(string hash) => new(hash);
    internal string GetHashForVerification() => _hash;   // only infrastructure can call this
}
```

### Repository interfaces (domain ports)

```csharp
// Domain/Repositories/IUserRepository.cs
public interface IUserRepository
{
    Task<User?> FindByIdAsync(Guid id, CancellationToken ct = default);
    Task<User?> FindByEmailAsync(string email, CancellationToken ct = default);
    Task<bool> ExistsByEmailAsync(string email, CancellationToken ct = default);
    Task SaveAsync(User user, CancellationToken ct = default);
    Task<User?> FindByOAuthAsync(string provider, string providerUserId, CancellationToken ct = default);
}
```

---

## Application layer

`Harmony.AuthService.Application/`

Uses **MediatR** for CQRS dispatch and **FluentValidation** for input validation. References only the `Domain` project.

### Folder structure

```
Application/
├── Commands/
│   ├── RegisterUser/
│   │   ├── RegisterUserCommand.cs           ← IRequest<RegisterUserResult>
│   │   ├── RegisterUserCommandHandler.cs    ← IRequestHandler<…>
│   │   ├── RegisterUserCommandValidator.cs  ← AbstractValidator<…>
│   │   └── RegisterUserResult.cs
│   ├── LoginUser/
│   │   ├── LoginUserCommand.cs
│   │   ├── LoginUserCommandHandler.cs
│   │   └── LoginUserResult.cs
│   ├── RefreshToken/
│   │   ├── RefreshTokenCommand.cs
│   │   └── RefreshTokenCommandHandler.cs
│   ├── RevokeToken/
│   │   └── RevokeTokenCommandHandler.cs
│   └── OAuthLogin/
│       ├── OAuthLoginCommand.cs
│       └── OAuthLoginCommandHandler.cs
├── Queries/
│   └── GetCurrentUser/
│       ├── GetCurrentUserQuery.cs
│       └── GetCurrentUserQueryHandler.cs
├── Ports/
│   ├── ITokenService.cs          ← GenerateAccessToken / GenerateRefreshToken / ValidateToken
│   ├── IPasswordHasher.cs        ← Hash / Verify
│   ├── IEventPublisher.cs        ← PublishAsync(IDomainEvent)
│   ├── ICacheService.cs          ← Get / Set / Delete (Redis)
│   └── IOAuthProvider.cs         ← ExchangeCode(code) → OAuthProfile
├── DTOs/
│   ├── OAuthProfile.cs
│   ├── TokenPairDto.cs
│   └── UserProfileDto.cs
├── Behaviors/
│   ├── ValidationBehavior.cs     ← FluentValidation pipeline
│   └── LoggingBehavior.cs        ← Structured logging per command
└── DependencyInjection.cs        ← AddApplication() extension method
```

### `RegisterUserCommandHandler`

```csharp
public sealed class RegisterUserCommandHandler
    : IRequestHandler<RegisterUserCommand, RegisterUserResult>
{
    private readonly IUserRepository _userRepo;
    private readonly IPasswordHasher _hasher;
    private readonly ITokenService   _tokens;
    private readonly IEventPublisher _events;

    public async Task<RegisterUserResult> Handle(
        RegisterUserCommand cmd, CancellationToken ct)
    {
        // 1. Guard — duplicate email
        if (await _userRepo.ExistsByEmailAsync(cmd.Email, ct))
            throw new DuplicateEmailException(cmd.Email);

        // 2. Value objects (validate in constructor)
        var email    = new Email(cmd.Email);
        var password = HashedPassword.FromHash(_hasher.Hash(cmd.Password));

        // 3. Create aggregate — raises UserRegisteredEvent
        var user = User.Create(email, password);

        // 4. Persist + publish domain events (outbox — same transaction)
        await _userRepo.SaveAsync(user, ct);
        foreach (var evt in user.PullDomainEvents())
            await _events.PublishAsync(evt, ct);

        // 5. Issue token pair
        var accessToken  = _tokens.GenerateAccessToken(user);
        var refreshToken = _tokens.GenerateRefreshToken();
        user.IssueRefreshToken(refreshToken.Token, refreshToken.ExpiresAt);
        await _userRepo.SaveAsync(user, ct);   // persist refresh token

        return new RegisterUserResult(user.Id, accessToken, refreshToken.Token);
    }
}
```

### Port interfaces

```csharp
// Ports/ITokenService.cs
public interface ITokenService
{
    string GenerateAccessToken(User user);
    (string Token, DateTimeOffset ExpiresAt) GenerateRefreshToken();
    ClaimsPrincipal? ValidateToken(string token);
}

// Ports/IPasswordHasher.cs
public interface IPasswordHasher
{
    string Hash(string plaintext);
    bool Verify(string plaintext, HashedPassword hashed);
}

// Ports/IOAuthProvider.cs
public interface IOAuthProvider
{
    string ProviderName { get; }
    Task<OAuthProfile> ExchangeCodeAsync(string code, CancellationToken ct);
}

public record OAuthProfile(string ProviderUserId, string Email, string? Name, string? AvatarUrl);
```

### Pipeline behaviors

| Behavior | Trigger | Action |
|---|---|---|
| `ValidationBehavior<TReq, TRes>` | Every command | Run FluentValidation; throw `ValidationException` on failure |
| `LoggingBehavior<TReq, TRes>` | Every command/query | Structured log with command type, duration, userId |

---

## Infrastructure layer

`Harmony.AuthService.Infrastructure/`

Implements every port defined in the Application layer.

### Folder structure

```
Infrastructure/
├── Persistence/
│   ├── AppDbContext.cs              ← EF Core DbContext
│   ├── Configurations/
│   │   ├── UserConfiguration.cs    ← Fluent API — maps value objects
│   │   └── RefreshTokenConfig.cs
│   ├── Repositories/
│   │   └── UserEfRepository.cs     ← implements IUserRepository
│   ├── Migrations/                  ← EF Core migrations
│   └── Outbox/
│       ├── OutboxMessage.cs
│       └── OutboxRelayService.cs    ← IHostedService — polls outbox → Kafka
├── Identity/
│   ├── JwtTokenService.cs           ← implements ITokenService (RS256)
│   ├── BCryptPasswordHasher.cs      ← implements IPasswordHasher
│   └── JwksEndpoint.cs             ← exposes /.well-known/jwks.json
├── Messaging/
│   ├── KafkaOutboxPublisher.cs      ← implements IEventPublisher (writes to outbox)
│   └── KafkaTopicConfig.cs
├── OAuth/
│   ├── GoogleOAuthClient.cs         ← implements IOAuthProvider
│   └── GitHubOAuthClient.cs         ← implements IOAuthProvider
├── Cache/
│   └── RedisCacheService.cs         ← implements ICacheService
└── DependencyInjection.cs           ← AddInfrastructure() extension method
```

### `JwtTokenService` — RS256 signing

```csharp
public sealed class JwtTokenService : ITokenService
{
    private readonly RsaSecurityKey _privateKey;
    private readonly JwtSettings    _settings;

    public string GenerateAccessToken(User user)
    {
        var claims = new[]
        {
            new Claim(JwtRegisteredClaimNames.Sub,   user.Id.ToString()),
            new Claim(JwtRegisteredClaimNames.Email, user.Email.Value),
            new Claim(JwtRegisteredClaimNames.Jti,   Guid.NewGuid().ToString()),
        };

        var creds   = new SigningCredentials(_privateKey, SecurityAlgorithms.RsaSha256);
        var token   = new JwtSecurityToken(
            issuer:   _settings.Issuer,
            audience: _settings.Audience,
            claims:   claims,
            expires:  DateTime.UtcNow.AddMinutes(15),
            signingCredentials: creds
        );
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    public (string Token, DateTimeOffset ExpiresAt) GenerateRefreshToken()
    {
        var bytes = RandomNumberGenerator.GetBytes(64);
        return (Convert.ToBase64String(bytes), DateTimeOffset.UtcNow.AddDays(7));
    }

    public ClaimsPrincipal? ValidateToken(string token) { /* RS256 validation */ }
}
```

### EF Core — value object mapping

```csharp
// Persistence/Configurations/UserConfiguration.cs
public class UserConfiguration : IEntityTypeConfiguration<User>
{
    public void Configure(EntityTypeBuilder<User> builder)
    {
        builder.HasKey(u => u.Id);

        builder.OwnsOne(u => u.Email, email =>
            email.Property(e => e.Value).HasColumnName("email")
                 .HasMaxLength(320).IsRequired());

        builder.OwnsOne(u => u.Password, pw =>
            pw.Property("_hash").HasColumnName("password_hash")
              .HasMaxLength(72).IsRequired(false));

        builder.OwnsOne(u => u.OAuthProvider, oauth =>
        {
            oauth.Property(o => o.Provider).HasColumnName("oauth_provider");
            oauth.Property(o => o.ProviderUserId).HasColumnName("oauth_provider_user_id");
        });
    }
}
```

### Outbox relay (`IHostedService`)

```csharp
public sealed class OutboxRelayService : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await ProcessBatchAsync(stoppingToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken);
        }
    }

    private async Task ProcessBatchAsync(CancellationToken ct)
    {
        using var scope = _serviceProvider.CreateScope();
        var db    = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var kafka = scope.ServiceProvider.GetRequiredService<IProducer<string, string>>();

        var messages = await db.OutboxMessages
            .Where(m => m.ProcessedAt == null)
            .OrderBy(m => m.CreatedAt)
            .Take(100)
            .ToListAsync(ct);

        foreach (var msg in messages)
        {
            await kafka.ProduceAsync(msg.Topic,
                new Message<string, string> { Key = msg.Key, Value = msg.Payload });
            msg.ProcessedAt = DateTimeOffset.UtcNow;
        }
        await db.SaveChangesAsync(ct);
    }
}
```

---

## API layer

`Harmony.AuthService.API/`

The thinnest possible layer — translate HTTP to MediatR, handle errors, configure middleware.

### Folder structure

```
API/
├── Controllers/
│   ├── AuthController.cs        ← Register, Login, Refresh, Revoke, Me
│   └── OAuthController.cs       ← Callback, redirect helpers
├── Middleware/
│   ├── JwtMiddleware.cs         ← Parse Bearer token → attach claims to HttpContext
│   └── GlobalExceptionHandler.cs ← Domain exceptions → ProblemDetails (RFC 7807)
├── Extensions/
│   └── ServiceCollectionExtensions.cs
├── appsettings.json
├── appsettings.Development.json
└── Program.cs
```

### `AuthController`

```csharp
[ApiController]
[Route("api/v1/auth")]
public sealed class AuthController(ISender sender) : ControllerBase
{
    [HttpPost("register")]
    [ProducesResponseType<RegisterUserResult>(201)]
    public async Task<IActionResult> Register(
        [FromBody] RegisterUserCommand cmd, CancellationToken ct)
    {
        var result = await sender.Send(cmd, ct);
        return CreatedAtAction(nameof(Me), new { }, result);
    }

    [HttpPost("login")]
    public async Task<LoginUserResult> Login(
        [FromBody] LoginUserCommand cmd, CancellationToken ct)
        => await sender.Send(cmd, ct);

    [HttpPost("refresh")]
    public async Task<TokenPairDto> Refresh(
        [FromBody] RefreshTokenCommand cmd, CancellationToken ct)
        => await sender.Send(cmd, ct);

    [HttpPost("revoke")]
    [Authorize]
    public async Task<IActionResult> Revoke(CancellationToken ct)
    {
        await sender.Send(new RevokeTokenCommand(User.GetUserId()), ct);
        return NoContent();
    }

    [HttpGet("me")]
    [Authorize]
    public async Task<UserProfileDto> Me(CancellationToken ct)
        => await sender.Send(new GetCurrentUserQuery(User.GetUserId()), ct);

    [HttpGet(".well-known/jwks.json")]
    [AllowAnonymous]
    public IActionResult Jwks([FromServices] JwksEndpoint jwks)
        => Ok(jwks.GetJwks());
}
```

### `GlobalExceptionHandler`

```csharp
public sealed class GlobalExceptionHandler : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(
        HttpContext ctx, Exception ex, CancellationToken ct)
    {
        var (status, title) = ex switch
        {
            DuplicateEmailException    => (409, "Email already registered"),
            InvalidCredentialsException=> (401, "Invalid credentials"),
            TokenExpiredException      => (401, "Token expired"),
            ValidationException        => (400, "Validation failed"),
            _                          => (500, "Internal server error")
        };

        ctx.Response.StatusCode = status;
        await ctx.Response.WriteAsJsonAsync(new ProblemDetails
        {
            Status = status,
            Title  = title,
            Detail = ex.Message
        }, ct);

        return true;
    }
}
```

---

## Authentication flows

### 1. Registration — `POST /api/v1/auth/register`

```
Client
  └─► AuthController.Register()
        └─► ValidationBehavior (FluentValidation)
              └─► RegisterUserCommandHandler
                    ├─► IUserRepository.ExistsByEmail()      → UserEfRepository → PostgreSQL
                    ├─► IPasswordHasher.Hash()               → BCryptPasswordHasher
                    ├─► User.Create()                        → raises UserRegisteredEvent
                    ├─► IUserRepository.Save()               → EF Core + outbox TX
                    ├─► IEventPublisher.Publish()            → writes to outbox table
                    └─► ITokenService.GenerateAccessToken()  → JwtTokenService (RS256)
  ◄─ 201 { accessToken, refreshToken, userId }
```

### 2. Login — `POST /api/v1/auth/login`

```
Client
  └─► AuthController.Login()
        └─► LoginUserCommandHandler
              ├─► IUserRepository.FindByEmail()
              ├─► IPasswordHasher.Verify()           → BCrypt constant-time
              ├─► ITokenService.GenerateAccessToken() → RS256 JWT (15 min)
              └─► ITokenService.GenerateRefreshToken() → 64-byte opaque (7 days)
  ◄─ 200 { accessToken, refreshToken }
```

### 3. Token refresh — `POST /api/v1/auth/refresh`

```
Client (JWT expired)
  └─► RefreshTokenCommandHandler
        ├─► IRefreshTokenRepository.Find(token)
        │     ├─ Not found or expired → 401
        │     └─ Already used → RevokeAllSessions() + 401 (replay attack)
        ├─► Mark old token as used
        ├─► ITokenService.GenerateRefreshToken()   → new opaque token
        └─► ITokenService.GenerateAccessToken()    → new RS256 JWT
  ◄─ 200 { accessToken, newRefreshToken }
```

### 4. OAuth2 — `GET /api/v1/auth/oauth/callback?provider=google&code=…`

```
Client → Google OAuth → GET /auth/oauth/callback?code=…
  └─► OAuthLoginCommandHandler
        ├─► IOAuthProvider.ExchangeCode(code)      → GoogleOAuthClient (HttpClient)
        │     └─► Google token endpoint → profile
        ├─► IUserRepository.FindByOAuth(provider, providerId)
        │     ├─ Found → existing user
        │     └─ Not found → User.CreateFromOAuth() → new user
        ├─► IUserRepository.Save()
        └─► ITokenService.Generate*()              → same JWT flow as login
  ◄─ 302 redirect to client with ?token=…
```

---

## JWT strategy

| Property | Value |
|---|---|
| Algorithm | RS256 (asymmetric) |
| Access token TTL | 15 minutes |
| Refresh token TTL | 7 days |
| Key rotation | RSA key pair in `appsettings.json` (or Azure Key Vault in prod) |
| Public key exposure | `GET /.well-known/jwks.json` — other services use this to verify without calling auth-svc |
| Claims | `sub` (userId), `email`, `jti` (unique token id), `exp`, `iat`, `iss`, `aud` |

**Other services verify JWTs locally** using the public key from the JWKS endpoint — they do not make a gRPC call to auth-svc on every request. This keeps auth-svc out of the hot path.

```yaml
# How other .NET services configure JWT validation
Authentication:
  JwtBearer:
    Authority: https://auth-svc/
    MetadataAddress: http://auth-svc/api/v1/auth/.well-known/jwks.json
    ValidAudience: harmony-api
    ValidIssuer: harmony-auth
```

---

## Domain events

### Published by this service

| Event | Kafka topic | Consumers |
|---|---|---|
| `UserRegisteredEvent` | `identity.user.registered` | `user-svc` (create profile), `search-svc` |
| `UserLoggedInEvent` | `identity.user.loggedin` | `notification-svc` (suspicious login alert) |
| `UserSessionRevokedEvent` | `identity.user.session.revoked` | `ws-gateway` (disconnect sessions) |

All events are written to the `outbox` table in the same EF Core `SaveChangesAsync` call as the business data, then relayed to Kafka by the background `OutboxRelayService`.

### Consumed by this service

None. auth-svc is a pure producer of identity events.

---

## Database

### Schema

```sql
-- users
CREATE TABLE users (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email                   VARCHAR(320) NOT NULL UNIQUE,
    password_hash           VARCHAR(72),                           -- null for OAuth-only users
    oauth_provider          VARCHAR(32),
    oauth_provider_user_id  VARCHAR(256),
    is_email_verified       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (oauth_provider, oauth_provider_user_id)
);

-- refresh_tokens
CREATE TABLE refresh_tokens (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token       TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    used_at     TIMESTAMPTZ,
    revoked     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_refresh_tokens_user ON refresh_tokens (user_id, expires_at);

-- outbox (transactional outbox for reliable Kafka publishing)
CREATE TABLE outbox_messages (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        TEXT NOT NULL,
    key          TEXT,
    payload      JSONB NOT NULL,
    processed_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_pending ON outbox_messages (created_at)
    WHERE processed_at IS NULL;
```

### EF Core migrations

```bash
cd src/Harmony.AuthService.Infrastructure
dotnet ef migrations add InitialCreate --startup-project ../Harmony.AuthService.API
dotnet ef database update --startup-project ../Harmony.AuthService.API
```

---

## Local development

### Prerequisites

- .NET SDK 8.0+
- Docker + Docker Compose
- (Optional) `dotnet-ef` CLI: `dotnet tool install --global dotnet-ef`

### Start infrastructure

```bash
docker-compose up -d
# Starts: PostgreSQL (5432), Kafka (9092), Redis (6379), Zookeeper (2181)
```

### Run the service

```bash
cd src/Harmony.AuthService.API
dotnet run
# API: http://localhost:8084
# JWKS: http://localhost:8084/api/v1/auth/.well-known/jwks.json
```

### Smoke tests

```bash
# Register
curl -X POST http://localhost:8084/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@harmony.app","password":"SuperSecret123!"}'

# Login
curl -X POST http://localhost:8084/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@harmony.app","password":"SuperSecret123!"}'

# Get profile (use accessToken from login response)
curl http://localhost:8084/api/v1/auth/me \
  -H "Authorization: Bearer <accessToken>"

# Get public key (used by other services)
curl http://localhost:8084/api/v1/auth/.well-known/jwks.json
```

---

## Testing strategy

| Layer | Type | Tool | Mocks |
|---|---|---|---|
| `Domain` | Unit | xUnit | None — pure C# |
| `Application` | Unit | xUnit + NSubstitute | All ports (`IUserRepository`, `ITokenService`, `IPasswordHasher`, `IEventPublisher`) |
| `Infrastructure` | Integration | xUnit + Testcontainers | Real PostgreSQL container, real Redis |
| `API` | Integration | `WebApplicationFactory<Program>` | Infrastructure replaced with in-memory fakes |

### Example — domain unit test

```csharp
[Fact]
public void User_Create_Raises_UserRegisteredEvent()
{
    var email    = new Email("alice@harmony.app");
    var password = HashedPassword.FromHash("$2b$12$...");

    var user   = User.Create(email, password);
    var events = user.PullDomainEvents();

    Assert.Single(events);
    Assert.IsType<UserRegisteredEvent>(events[0]);
    Assert.Equal("alice@harmony.app", ((UserRegisteredEvent)events[0]).Email);
}

[Fact]
public void Email_ValueObject_Rejects_Invalid_Format()
{
    Assert.Throws<InvalidEmailException>(() => new Email("not-an-email"));
    Assert.Throws<InvalidEmailException>(() => new Email(""));
}
```

### Example — application handler unit test

```csharp
[Fact]
public async Task RegisterUserHandler_Throws_DuplicateEmailException_When_Email_Exists()
{
    var repo   = Substitute.For<IUserRepository>();
    var hasher = Substitute.For<IPasswordHasher>();
    repo.ExistsByEmailAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
        .Returns(true);

    var handler = new RegisterUserCommandHandler(repo, hasher, /* tokens */ null!, /* events */ null!);
    var cmd     = new RegisterUserCommand("alice@harmony.app", "password");

    await Assert.ThrowsAsync<DuplicateEmailException>(
        () => handler.Handle(cmd, CancellationToken.None));
}
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `ConnectionStrings__DefaultConnection` | — | PostgreSQL connection string |
| `Redis__ConnectionString` | `localhost:6379` | Redis connection |
| `Kafka__BootstrapServers` | `localhost:9092` | Kafka brokers |
| `Jwt__Issuer` | `harmony-auth` | JWT `iss` claim |
| `Jwt__Audience` | `harmony-api` | JWT `aud` claim |
| `Jwt__PrivateKeyPem` | — | RSA private key PEM (base64) — **secret** |
| `Jwt__PublicKeyPem` | — | RSA public key PEM (base64) |
| `Jwt__AccessTokenExpiryMinutes` | `15` | Access token TTL |
| `Jwt__RefreshTokenExpiryDays` | `7` | Refresh token TTL |
| `OAuth__Google__ClientId` | — | Google OAuth2 client ID |
| `OAuth__Google__ClientSecret` | — | Google OAuth2 client secret — **secret** |
| `OAuth__Google__RedirectUri` | — | Registered redirect URI |
| `OAuth__GitHub__ClientId` | — | GitHub OAuth2 client ID |
| `OAuth__GitHub__ClientSecret` | — | GitHub OAuth2 client secret — **secret** |
| `Outbox__PollIntervalMs` | `500` | Outbox relay poll interval |
| `ASPNETCORE_ENVIRONMENT` | `Development` | `Development` / `Staging` / `Production` |
| `ASPNETCORE_URLS` | `http://+:8084` | Bind address |

---

## Related services

| Service | Relationship |
|---|---|
| `user-svc` | Consumes `identity.user.registered` to create the user profile |
| `community-svc` | Verifies JWT using JWKS endpoint; never calls auth-svc per-request |
| `messaging-svc` | Same JWT verification pattern |
| `ws-gateway` | Verifies JWT on WebSocket upgrade; consumes `identity.user.session.revoked` to disconnect |
| `notification-svc` | Consumes `identity.user.loggedin` for suspicious login alerts |
| `api-gateway` | Optionally validates token at gateway level (JWKS) before forwarding |

---

*Part of the Harmony platform monorepo — see the root [README](../../README.md) for the full architecture overview.*