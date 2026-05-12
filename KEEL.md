# KEEL

## Active repo

`noogrub/fieldnet`

## Active project

`fieldnet`

## Canonical boot source

`noogrub/fieldnet/README.md`

## Filename context

Reset on boot.

## Project identity

`fieldnet` is the small-Linux-device edge-node implementation for FieldNet / NopeNet concepts.

It is intended for nodes such as Raspberry Pi-class devices that can host transport, local logic, demonstrations, visualization adapters, scripts, and runtime logs.

## Current status

Early scaffolding.

The repo is a deployment-oriented edge-node layer, not the original MNIST abstention demo and not the STM32 PLL sensor implementation.

## Relationship to sibling repos

- `noogrub/nopenet` contains the Phase 0 abstention-integrity demonstration.
- `noogrub/fieldnet_PLL` contains the STM32L432KC PLL / SEE / SET ingestion sensor project.
- `noogrub/lewm_pll_mini` contains the LeWM / PLL latent-world-model research scaffold.

## Boot rule

When John says `load Keel for fieldnet`, read `noogrub/fieldnet/README.md` first, then this file.

Do not route through `chat-repo` before reading this repo's README.

## Immediate continuity task

Inspect the actual code structure and determine which parts implement:

- transport
- FieldNet logic
- NopeNet logic
- demos
- visualization adapters
- runtime logging

Then create or update project-local continuity files only after inspecting the current repo contents.
