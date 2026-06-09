SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

ROOT_MODULE := github.com/gopherex/synclog
# Max major allowed. v2+ needs semantic import versioning (/vN in the module
# path), which we don't support yet — keep releases on v0/v1.
MAX_MAJOR := 1

.PHONY: help gen fmt test test-ts release

help:
	@echo "make gen          - regenerate transport/*.pb.go via easyp"
	@echo "make test         - gofmt check + go vet + go test ./..."
	@echo "make test-ts      - npm ci + build/typecheck/test the TypeScript workspace"
	@echo "make release      - interactive tag-driven release (Go module + @gopherex/synclog share one version)"

# --- codegen / build ---------------------------------------------------------

gen:
	cd protocols && easyp -cfg easyp.go.yaml mod update && easyp -cfg easyp.go.yaml mod vendor && easyp -cfg easyp.go.yaml generate && easyp -cfg easyp.ts.yaml generate

fmt:
	gofmt -w .

# --- test --------------------------------------------------------------------

test:
	gofmt -l .
	go vet ./...
	go test -race ./...

test-ts:
	npm ci
	npm run --workspaces build
	npm run --workspaces typecheck
	npm run --workspaces test

# --- release -----------------------------------------------------------------
# One version drives everything: the Go module is tagged vX.Y.Z, and the
# @gopherex/synclog npm package is bumped to the same X.Y.Z. Pushing the tag
# triggers .github/workflows/release.yml, which creates a GitHub release and
# publishes the npm package to GitHub Packages.
release:
	@set -euo pipefail
	cd "$$(git rev-parse --show-toplevel)"

	if [ -n "$$(git status --porcelain)" ]; then
	  echo "✗ Working tree is not clean — commit or stash first:"
	  git status --short
	  exit 1
	fi

	cur="$$(git tag -l 'v[0-9]*.[0-9]*.[0-9]*' | sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)"
	cur="$${cur:-0.0.0}"
	head="$$(git rev-parse --short HEAD)"
	echo "Latest release: v$$cur    HEAD: $$head"
	echo
	echo "  1) bump version"
	echo "  2) recreate last tag (v$$cur) on HEAD   [force]"
	echo "  3) cancel"
	read -r -p "> " action

	set_npm_version() { # $1 = version (without v); bumps workspace + lockfile
	  npm version "$$1" --no-git-tag-version --workspaces >/dev/null
	}

	case "$$action" in
	1)
	  IFS=. read -r MA MI PA <<< "$$cur"
	  echo
	  echo "  1) major  -> v$$((MA+1)).0.0"
	  echo "  2) minor  -> v$$MA.$$((MI+1)).0"
	  echo "  3) patch  -> v$$MA.$$MI.$$((PA+1))"
	  read -r -p "> " comp
	  case "$$comp" in
	    1) MA=$$((MA+1)); MI=0; PA=0 ;;
	    2) MI=$$((MI+1)); PA=0 ;;
	    3) PA=$$((PA+1)) ;;
	    *) echo "Aborted."; exit 0 ;;
	  esac
	  if [ "$$MA" -gt "$(MAX_MAJOR)" ]; then
	    echo "✗ v$$MA requires semantic import versioning (/v$$MA in the module path)."
	    echo "  Not supported yet — stay on v0/v1."
	    exit 1
	  fi
	  new="$$MA.$$MI.$$PA"
	  echo
	  echo "Release v$$new — will:"
	  echo "  - set npm version $$new in @gopherex/synclog (+ lockfile)"
	  echo "  - commit 'release v$$new'"
	  echo "  - create tag v$$new and push HEAD + tag (triggers CI release)"
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }

	  set_npm_version "$$new"
	  git add -A
	  git diff --cached --quiet || git commit -m "release v$$new"
	  git tag -a "v$$new" -m "v$$new"
	  git push origin HEAD
	  git push origin "v$$new"
	  echo "✓ Released v$$new."
	  ;;
	2)
	  if [ "$$cur" = "0.0.0" ] && ! git tag -l 'v0.0.0' | grep -q .; then
	    echo "✗ No release tag to recreate."; exit 1
	  fi
	  echo
	  echo "Will DELETE and recreate tag v$$cur on $$head, then force-push."
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }
	  git tag -d "v$$cur" 2>/dev/null || true
	  git push origin ":refs/tags/v$$cur" 2>/dev/null || true
	  git tag -a "v$$cur" -m "v$$cur"
	  git push origin --force "v$$cur"
	  echo "✓ Recreated v$$cur on $$head."
	  ;;
	*)
	  echo "Cancelled."
	  ;;
	esac
