# Releasing

Releases are made by pushing a tag `v<version>` on `main`. The
[release workflow](.github/workflows/release.yml) then:

1. checks that the tag equals the version in `pyproject.toml` and that the tagged commit is on
   `main`, and that `CI passed` succeeded on that commit (it waits up to 45 minutes for a CI run
   that is still in progress, and publishes nothing otherwise);
2. builds the sdist and wheel, runs `twine check --strict` and
   [`check_dist.py`](.github/scripts/check_dist.py) (package-only wheel, metadata without e-mail
   addresses, version equal to the tag and to `pyproject.toml`);
3. publishes to TestPyPI, if the repository variable `PUBLISH_TO_TESTPYPI` is `true`;
4. publishes to PyPI, if the repository variable `PUBLISH_TO_PYPI` is `true`;
5. creates a GitHub Release with the distributions, a `SHA256SUMS` file and the release notes
   taken from `CHANGELOG.md`.

A step only runs if the steps before it succeeded or were skipped. Both publish jobs use
[trusted publishing](https://docs.pypi.org/trusted-publishers/): there are no PyPI tokens in the
repository, and only those two jobs get the `id-token: write` permission.

## Versions

Versions follow [PEP 440](https://peps.python.org/pep-0440/). A version with a pre-release suffix
(`0.3.0rc1`, `0.3.0b2`, `0.3.0.dev1`) is published as a GitHub pre-release; pip only installs it
when asked for explicitly (`pip install --pre` or `==0.3.0rc1`).

The version is defined once, as `version` in `pyproject.toml`. `cosmosdb_datasource.__version__`
reads it from the installed distribution's metadata.

## Making a release

1. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`, add an empty
   `## [Unreleased]` above it, and update the links at the end of the file. A pre-release may keep
   its notes under `[Unreleased]`: the workflow uses that section when there is no section for the
   pre-release version.
2. Set the version:

   ```bash
   uv version X.Y.Z          # updates pyproject.toml and uv.lock
   ```
3. Open a pull request, and merge it once CI is green.
4. Once CI has passed on the merge commit on `main`, tag it and push the tag:

   ```bash
   git switch main && git pull
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

5. Follow the run under **Actions -> Release** and approve the `testpypi` and `pypi` deployments
   there (**Review deployments**).

A file that was uploaded to PyPI cannot be replaced, even after deleting it. If a published
release is broken, [yank](https://pypi.org/help/#yanked) it on PyPI and release a new version.

### Deleting a release

To remove a GitHub Release together with its tag, for example after testing the workflow with a
pre-release tag, set the `release tags` ruleset to **Disabled** first (see
[Repository rules](#repository-rules)), and back to **Active** afterwards:

```bash
gh release delete vX.Y.ZrcN --cleanup-tag --yes
```

## One-time setup

### PyPI and TestPyPI trusted publishers

Do this once on [PyPI](https://pypi.org/manage/account/publishing/) and once on
[TestPyPI](https://test.pypi.org/manage/account/publishing/) (separate accounts). Before the first
upload the project does not exist yet, so add a *pending* publisher under **Your account ->
Publishing**:

| Field | PyPI | TestPyPI |
|---|---|---|
| PyPI project name | `cosmosdb-spark-datasource` | `cosmosdb-spark-datasource` |
| Owner | `Mmodarre` | `Mmodarre` |
| Repository name | `cosmosdb-spark-datasource` | `cosmosdb-spark-datasource` |
| Workflow name | `release.yml` | `release.yml` |
| Environment name | `pypi` | `testpypi` |

The first successful upload turns the pending publisher into a normal one. After that it is
listed under the project's **Settings -> Publishing**.

### GitHub environments

The environments `pypi` and `testpypi` (**Settings -> Environments**) accept deployments only from
tags `v*`, and each upload waits for an approval by `@Mmodarre` (self-review allowed,
administrators cannot bypass it).

### Repository rules

The rulesets under **Settings -> Rules -> Rulesets** have no bypass list, so they apply to
administrators as well:

- `main`: changes arrive only through squash-merged pull requests. `CI passed`, `Emulator (vnext)`
  and `Emulator (legacy)` must pass on a branch that is up to date with `main`
  (`gh pr update-branch`), and review threads must be resolved. No force pushes, no deletion.
- `release tags`: tags `v*` cannot be deleted or moved.

Workflows may use GitHub-owned actions and `astral-sh/setup-uv`, `codecov/codecov-action` and
`pypa/gh-action-pypi-publish`, each pinned to a full commit SHA. A new third-party action must
first be allowed under **Settings -> Actions -> General**.

### Repository variables

In **Settings -> Secrets and variables -> Actions -> Variables**, set:

| Variable | Value | Effect |
|---|---|---|
| `PUBLISH_TO_TESTPYPI` | `true` | the release workflow uploads to TestPyPI |
| `PUBLISH_TO_PYPI` | `true` | the release workflow uploads to PyPI |

Without these variables (or with any other value) the workflow only builds, checks and creates the
GitHub Release.

To try the whole path first, set only `PUBLISH_TO_TESTPYPI=true` and push a pre-release tag, then
install from TestPyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ \
  "cosmosdb-spark-datasource==X.Y.ZrcN"
```
