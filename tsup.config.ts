import { defineConfig } from "tsup";

// Bundles the hand-written facade (src/) together with the generated core
// (core/src/) into a single dual ESM + CJS package. Bundling sidesteps the
// per-file extension mismatch between the facade (NodeNext-style) and the
// generated core (extensionless imports), and means consumers never see the
// internal core/ layout.
export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  treeshake: true,
  target: "node18",
});
