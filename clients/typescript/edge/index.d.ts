// SPDX-License-Identifier: Apache-2.0
import type {EdgeBundle, BundleManifest} from '../dist/src/models.js';
export type EdgeRow = Record<string, unknown> & {id: string; $dist?: number};
export interface SuggestOptions {
  collapseBy: string | string[];
  topK?: number;
  candidates?: number;
  missing?: 'own' | 'together' | 'drop' | {mode?: 'own' | 'together' | 'drop'; values?: (string | number | boolean)[]};
}
export interface EdgeSearch {
  manifest: BundleManifest;
  free(): void;
  suggest(text: string, options: SuggestOptions): {rows: EdgeRow[]; groups: {key: unknown; by: string | null; rows: EdgeRow[]}[]};
}
/** Browser bundlers must support WASM imports (micropuffer 0.2.0). Node: --experimental-wasm-modules. */
export function loadBundle(input: EdgeBundle | string | URL, options?: {field?: string}): Promise<EdgeSearch>;
