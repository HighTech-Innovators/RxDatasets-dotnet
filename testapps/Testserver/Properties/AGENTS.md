# testapps/Testserver/Properties

## Files

- `launchSettings.json` — profiles `http2` (`http://localhost:10080`) and `http1` (`http://localhost:18080`), both `ASPNETCORE_ENVIRONMENT=Development`.

## Gotchas

- Outside `Testing`, `../Program.cs` binds both ports itself, overriding `applicationUrl`, so the two profiles behave the same. The ports clash with `testapps/Host`.
