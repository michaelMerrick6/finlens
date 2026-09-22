import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import ts from 'typescript';

export function typescriptLoader(dependencies = {}, globals = {}) {
  const modules = new Map();
  function load(filename) {
    if (modules.has(filename)) return modules.get(filename);
    const exports = {};
    modules.set(filename, exports);
    const code = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
      compilerOptions: { esModuleInterop: true, module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, {
      exports, Date, URL, AbortSignal, Error, setTimeout, clearTimeout,
      process: { env: {} }, ...globals,
      require(name) {
        if (name in dependencies) return dependencies[name];
        if (name === 'server-only') return {};
        const resolved = name.startsWith('@/') ? `src/${name.slice(2)}`
          : name.startsWith('.') ? path.join(path.dirname(filename), name) : null;
        if (!resolved) throw new Error(`Unexpected dependency: ${name}`);
        if (resolved.endsWith('.json')) return JSON.parse(fs.readFileSync(resolved, 'utf8'));
        return load(`${resolved}.ts`);
      },
    });
    return exports;
  }
  return load;
}
