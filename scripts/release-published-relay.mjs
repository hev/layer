#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const TAG = /^v(\d+)\.(\d+)\.(\d+)$/;
const SOURCE_MARKER = /<!--\s*source=([0-9a-f]{40})\s+base=[^>]+-->/i;

export const notesSha256 = (body) =>
	createHash("sha256").update(body ?? "", "utf8").digest("hex");

export const sourceShaFromNotes = (body) => {
	const match = (body ?? "").match(SOURCE_MARKER);
	if (!match) throw new Error("published release notes are missing the layer-pro source marker");
	return match[1].toLowerCase();
};

export const expectedReleaseBranches = (tagName) => {
	const match = tagName.match(TAG);
	if (!match) throw new Error(`release tag must be stable vX.Y.Z (got: ${tagName})`);
	return [`v${match[1]}.${match[2]}`, `release/v${match[1]}.${match[2]}`];
};

export const buildDispatch = ({ repository, release, tagSha }) => {
	if (repository !== "hev/layer") {
		throw new Error(`release relay only accepts hev/layer (got: ${repository})`);
	}
	if (!release || release.draft || !release.published_at) {
		throw new Error("release relay requires a published, non-draft release");
	}
	if (!Number.isInteger(release.id) || release.id <= 0) {
		throw new Error("published release id must be a positive integer");
	}
	if (!/^[0-9a-f]{40}$/i.test(tagSha ?? "")) {
		throw new Error(`release tag must resolve to a commit SHA (got: ${tagSha ?? "<missing>"})`);
	}
	const branches = expectedReleaseBranches(release.tag_name);
	if (!branches.includes(release.target_commitish)) {
		throw new Error(
			`release ${release.tag_name} must target ${branches.join(" or ")} (got: ${release.target_commitish})`,
		);
	}

	return {
		event_type: "layer-release-published",
		client_payload: {
			source_repository: repository,
			release_id: release.id,
			tag: release.tag_name,
			tag_sha: tagSha.toLowerCase(),
			target_branch: release.target_commitish,
			source_sha: sourceShaFromNotes(release.body),
			notes_sha256: notesSha256(release.body),
			published_at: release.published_at,
			release_url: release.html_url,
		},
	};
};

const githubRequest = async ({ token, repository, path, method = "GET", body }) => {
	const response = await fetch(`https://api.github.com/repos/${repository}${path}`, {
		method,
		headers: {
			Accept: "application/vnd.github+json",
			Authorization: `Bearer ${token}`,
			"X-GitHub-Api-Version": "2022-11-28",
			"User-Agent": "hevlayer-release-relay",
		},
		body: body === undefined ? undefined : JSON.stringify(body),
	});
	if (!response.ok) {
		throw new Error(`${method} ${repository}${path}: ${response.status} ${await response.text()}`);
	}
	return response.status === 204 ? null : response.json();
};

const resolveTagCommit = async ({ token, repository, tagName }) => {
	let object = (await githubRequest({
		token,
		repository,
		path: `/git/ref/tags/${encodeURIComponent(tagName)}`,
	})).object;
	for (let depth = 0; object.type === "tag" && depth < 4; depth += 1) {
		object = (await githubRequest({
			token,
			repository,
			path: `/git/tags/${object.sha}`,
		})).object;
	}
	if (object.type !== "commit") {
		throw new Error(`release tag ${tagName} resolves to ${object.type}, not a commit`);
	}
	return object.sha;
};

const parseArgs = (argv) => {
	const args = { dryRun: false, eventPath: process.env.GITHUB_EVENT_PATH, tagSha: undefined };
	for (let index = 0; index < argv.length; index += 1) {
		const arg = argv[index];
		if (arg === "--dry-run") args.dryRun = true;
		else if (arg === "--event") args.eventPath = argv[++index];
		else if (arg === "--tag-sha") args.tagSha = argv[++index];
		else throw new Error(`unknown argument: ${arg}`);
	}
	if (!args.eventPath) throw new Error("GITHUB_EVENT_PATH or --event is required");
	return args;
};

export const main = async (argv = process.argv.slice(2), env = process.env) => {
	const args = parseArgs(argv);
	const event = JSON.parse(await readFile(args.eventPath, "utf8"));
	if (event.action && event.action !== "published") {
		throw new Error(`release relay requires action=published (got: ${event.action})`);
	}
	const repository = event.repository?.full_name ?? env.GITHUB_REPOSITORY;
	const readToken = env.GITHUB_TOKEN ?? env.GH_TOKEN;
	const dispatchToken = env.LAYER_PRO_DISPATCH_TOKEN;
	let tagSha = args.tagSha;
	if (!tagSha) {
		if (args.dryRun) throw new Error("--tag-sha is required with --dry-run");
		if (!readToken) throw new Error("GITHUB_TOKEN is required to resolve the published tag");
		tagSha = await resolveTagCommit({ token: readToken, repository, tagName: event.release?.tag_name });
	}
	const dispatch = buildDispatch({ repository, release: event.release, tagSha });
	console.log(JSON.stringify(dispatch, null, 2));
	if (args.dryRun) return dispatch;
	if (!dispatchToken) throw new Error("LAYER_PRO_DISPATCH_TOKEN is required");
	await githubRequest({
		token: dispatchToken,
		repository: "hev/layer-pro",
		path: "/dispatches",
		method: "POST",
		body: dispatch,
	});
	console.error(`relayed ${dispatch.client_payload.tag} to hev/layer-pro`);
	return dispatch;
};

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
	main().catch((error) => {
		console.error(error.message);
		process.exit(1);
	});
}
