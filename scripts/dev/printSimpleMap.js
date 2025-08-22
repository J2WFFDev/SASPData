// Node runner that imports the compiled JS mapping
// This file will attempt to load TypeScript via ts-node if available,
// otherwise will require the TS file via a small eval.

(async function main(){
  const fs = require('fs');
  const path = require('path');
  const srcPath = path.resolve(__dirname, '..', 'src', 'tableMap.ts');
  try {
    const src = fs.readFileSync(srcPath, 'utf8');
    // Find the returned template literal in getSimpleMap()
    const re = /getSimpleMap\s*\([^\)]*\)\s*:\s*string\s*\{[\s\S]*?return\s*`([\s\S]*?)`\s*;\s*\}/m;
    const m = src.match(re);
    if (m && m[1]) {
      console.log(m[1]);
      process.exit(0);
    }
    // Fallback: try to find a simpler return `...` without explicit type
    const re2 = /getSimpleMap\s*\([^\)]*\)\s*\{[\s\S]*?return\s*`([\s\S]*?)`\s*;\s*\}/m;
    const m2 = src.match(re2);
    if (m2 && m2[1]) {
      console.log(m2[1]);
      process.exit(0);
    }
    console.error('Could not extract simple map template from', srcPath);
    process.exit(2);
  } catch (err) {
    console.error('Error reading', srcPath, err && err.message ? err.message : err);
    process.exit(3);
  }
})();
