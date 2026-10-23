# Hosting the comparison website

GitHub Pages fits this static site and the repository's existing public audience. Publish only the reviewed output of `website/dist` on the dedicated `gh-pages` branch. The application source remains on its review branch until the pull request is merged separately.

The project-site address is **https://hardbyte.github.io/postgresql-job-queue-benchmarking/**. The site uses relative asset links and directory pages under `/postgresql-job-queue-benchmarking/`; the build includes `.nojekyll`. No custom domain, DNS change, server runtime, Cloudflare account or new credential is required.

## Verified repository setup

Read-only inspection on 2026-10-24 found a public repository with default branch `main`, existing credential permission `ADMIN`, and GitHub Actions enabled. The Pages API returned HTTP 404, and neither `gh-pages` nor a deployment environment existed. The checked-in base had harness CI, not an existing website deployment.

The owner subsequently requested a working published link and a pull request. The publication route is therefore a Pages branch build using the existing repository identity. It does not require configuring human-reviewer environments, granting a service access, creating a personal token, changing repository visibility, or merging the source pull request.

## Review and publish a build

From the source checkout:

```bash
npm --prefix website run lint
node --test --test-isolation=none website/test.cjs
node website/build.cjs
python3 -m http.server 8765 --bind 127.0.0.1 --directory website/dist
```

Review `http://127.0.0.1:8765/`, including current-source provenance, profile comparisons, mobile navigation and the simulation boundary. Check that the distribution contains only intended public static assets: no credentials, source checkout metadata, local screenshots, test outputs, raw runs or historical numerical reports. The lab is a teaching model; it is not a benchmark result.

After the exact build is approved for publication:

1. Create an isolated publication checkout on an orphan `gh-pages` branch. Copy only the reviewed contents of `website/dist` into its root; reject symlinks. Retain `.nojekyll`. Record the generated-file SHA-256 manifest with the publication receipt so the deployed content can be matched to the approved build.
2. Commit and push that publication branch with the existing GitHub credential. Do not force-push an existing publication branch; fetch its current state and make an ordinary update if it already exists. Do not push the source review branch as part of the publication operation.
3. If Pages is not configured, the repository administrator can configure **Settings → Pages → Source: Deploy from a branch → `gh-pages` → `/ (root)`**. The equivalent existing-credential API request is `POST /repos/hardbyte/postgresql-job-queue-benchmarking/pages` with `{"build_type":"legacy","source":{"branch":"gh-pages","path":"/"}}`. Read the current settings first and resolve any conflict before changing them. This changes the hosting source, not repository visibility, access grants, a custom domain or DNS.
4. Wait for the Pages build associated with the new publication commit. A successful push is not evidence that the website is live.
5. Verify the actual HTTPS root, stylesheet/script assets, a system profile, comparison navigation and the lab. Retain the built commit and verified URL in the delivery receipt. Source changes and the pull request remain independently reviewable; do not merge the pull request as part of publishing the preview.

Pages will publish future pushes to its configured `gh-pages` source. That branch is the explicit publication boundary; a push to the ordinary source review branch does not update the site. Before every future publication, rebuild, validate and review exactly what will be copied there.

## Workflow and permissions

[`.github/workflows/website-pages.yml`](../.github/workflows/website-pages.yml) validates relevant pull requests and manual dispatches, with artifact packaging only. It has `contents: read`, lint/test/build/browser accessibility steps and a 14-day downloadable artifact. It has no publication step, deployment environment, Pages enablement or write permission. Official actions are pinned to verified revisions. Merely pushing or merging this workflow does not publish the website.

Publishing the reviewed generated branch uses the owner's existing Git write access. Initial Pages configuration needs the repository administrator's existing Pages/administration settings access. No additional security-sensitive permission grant or persistent credential is required. If the existing credential cannot perform the authorized operation, stop and report that limitation rather than creating a credential or broadening access.

## Alternative host

The same static distribution can be deployed to Cloudflare later if desired, but no Cloudflare project, account integration, token or domain has been created for this site. GitHub Pages avoids introducing another service and supplies the requested working project URL.

## Official references

- [GitHub Pages static hosting and project-site URLs](https://docs.github.com/en/pages/getting-started-with-github-pages/what-is-github-pages)
- [Publishing from a branch and folder](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site)
- [Pages REST API: inspect/configure sites and inspect builds](https://docs.github.com/en/rest/pages/pages)
- [Manual workflow requirements](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow)
