# @gopherex/synclog

Transport-free frontend SDK and generated protobuf types for
[`github.com/gopherex/synclog`](https://github.com/gopherex/synclog).

Install from GitHub Packages:

```ini
@gopherex:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

```bash
npm install @gopherex/synclog
```

The package exports:

- `SyncClient` - catch-up, TOO_LONG snapshot recovery, subscribe, apply-before-ack.
- `SyncGatewayTransport` - implement this with any network transport.
- `ProjectionStore` - implement this with your local app store.
- `MemorySyncStore` - reference/test store.
- Generated `synclog.v1` protobuf types and schemas.

See the repository README for backend embedding, release, and full usage docs.
