# datasets

https://dataresearchcenter.org/library


Current version: 2025.6.4.0


## Versioning

This project uses [Calendar Versioning (CalVer)](https://calver.org/) with the pattern:

```
YYYY.MM.DD.PATCH
```

- `YYYY` - Full year (e.g., 2025)
- `MM` - Month (1-12, no leading zero)
- `DD` - Day (1-31, no leading zero)
- `PATCH` - Patch number for same-day releases (starts at 0)

Example: `2025.6.4.0` = June 4th, 2025, first release of the day.

### Bumping the version

The project uses [bumpver](https://github.com/mbarkhau/bumpver) configured in `pyproject.toml`.

```bash
# Bump to current date (resets PATCH to 0)
bumpver update

# Bump only the PATCH number (for multiple releases on same day)
bumpver update --patch

# Preview changes without applying them
bumpver update --dry

# Preview patch bump without applying
bumpver update --patch --dry
```

When you run `bumpver update`, it will:
1. Update the version in `pyproject.toml` and `README.md`
2. Create a git commit with message: `🔖 bump version X -> Y`
3. Create a git tag with the new version
4. Push the commit and tag to the remote repository
