# Releasing

The version comes from the git tag and nowhere else: `hatch-vcs` derives it,
so there is no number to bump in a file and no way for a file to disagree with
the tag.

## Once, before the first release

1. **Create a PyPI *pending* publisher.** The project does not exist on PyPI
   until the first upload, so there are no project settings to add a publisher
   to yet. Use <https://pypi.org/manage/account/publishing/>, which is the
   flow for exactly this case -- it reserves the name and authorises the
   workflow in one step:

   | Field | Value |
   | --- | --- |
   | PyPI project name | `taskflow-meter` |
   | Owner | the GitHub org or user |
   | Repository | `taskflow.meter` |
   | Workflow name | `release.yml` |
   | Environment name | `pypi` |

   Note the repository is `taskflow.meter` while the project is
   `taskflow-meter`; they differ, and PyPI matches on the repository.

   After the first successful upload the pending publisher becomes an ordinary
   one under the project's *Publishing* settings, and later releases need
   nothing further.

2. **Create the `pypi` environment** in the repository's settings. The release
   workflow names it, and `id-token: write` is granted only to the job that
   uses it.

No API token is stored anywhere. If either step is missing the publish job
fails with a permissions error rather than falling back to something less
safe.

## Each release

```bash
# 1. Everything green, on the commit you intend to ship.
uvx tox

# 2. Move the Unreleased section of CHANGELOG.md under the new version.
$EDITOR CHANGELOG.md
git commit -am "Prepare the 1.0.0 release"

# 3. Tag it. This is the version.
git tag -a v1.0.0 -m "1.0.0"

# 4. Push. The tag is what starts the release.
git push origin main
git push origin v1.0.0
```

`release.yml` then builds the sdist and wheel, **checks the built version
matches the tag** and fails if it does not, publishes to PyPI through trusted
publishing, and creates a GitHub release with the artifacts attached.

## Checking a release without publishing one

```bash
uv build
uvx twine check dist/*
uv run --isolated --no-project --with dist/*.whl \
  python -c "import taskflow_meter; print(taskflow_meter.__version__)"
```

An untagged tree builds as `0.0.1.devN+g<sha>`, which is expected and is why
the workflow compares against the tag before publishing.

## After

The `conformance` workflow runs weekly against the host frameworks' current
releases. A failure there is the earliest warning that a new FastAPI or Django
has changed something under a mounted application -- worth watching in the
days after a release.
