#!/usr/bin/env node
// Run the vendored Tableau formula translator over a JSON batch without
// modifying the bundle. Input is either an array of formula objects or
// {"formulas":[...]}; each object must have a string `formula` and may carry
// arbitrary identifying metadata, which is copied to the result.
//
// Usage:
//   printf '%s' '[{"name":"x","formula":"SUM([Sales])"}]' |
//     node scripts/formula-audit.mjs
//   node scripts/formula-audit.mjs --input formulas.json

import {
  appendFileSync,
  copyFileSync,
  mkdtempSync,
  readFileSync,
  rmSync
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const SKILL = resolve(HERE, '..');
const BUNDLE = join(SKILL, 'converter', 'tableau.mjs');
const TABLE = join(SKILL, 'refs', 'functions.json');
// Same keyword source as lib/calc_coverage.rb: `IN (`, `AND (`, `NOT (` are
// operators, not calls, and must not be scored as unmapped functions.
const CATALOG = join(HERE, 'lib', 'tableau_functions.json');
const STATUSES = ['spec', 'verify', 'chart_only', 'rls', 'not_converted', 'unmapped'];
const TRANSLATED = new Set(['spec', 'verify', 'chart_only', 'rls']);
const SHIM = '\nexport { tableauFormulaToSigma as __auditToSigma, tableauWindowToSigmaChart as __auditWindow, tableauFormulaIsRls as __auditIsRls };\n';

function fail(message) {
  console.error(`formula-audit: ${message}`);
  process.exitCode = 2;
}

function parseArgs(argv) {
  let input = null;
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--input') {
      if (input !== null || !argv[i + 1]) throw new Error('--input requires exactly one file path');
      input = argv[++i];
    } else if (argv[i] === '--help' || argv[i] === '-h') {
      process.stdout.write('usage: formula-audit.mjs [--input formulas.json]\n');
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${argv[i]}`);
    }
  }
  return input;
}

function stdinText() {
  return readFileSync(0, 'utf8');
}

// Mask comments, quoted strings, and bracketed field references before
// collecting calls. This mirrors the important property of CalcCoverage:
// function-looking text inside a literal or field caption is not a function.
function tableauFunctionNames(formula, keywords) {
  const text = String(formula || '');
  let masked = '';
  for (let i = 0; i < text.length;) {
    const ch = text[i];
    if (ch === '"' || ch === "'") {
      const close = text.indexOf(ch, i + 1);
      i = close < 0 ? text.length : close + 1;
      masked += ' ';
    } else if (ch === '[') {
      const close = text.indexOf(']', i + 1);
      i = close < 0 ? text.length : close + 1;
      masked += ' ';
    } else if (text.slice(i, i + 2) === '//') {
      const close = text.indexOf('\n', i + 2);
      i = close < 0 ? text.length : close;
      masked += ' ';
    } else if (text.slice(i, i + 2) === '/*') {
      let depth = 1;
      i += 2;
      while (i < text.length && depth > 0) {
        if (text.slice(i, i + 2) === '/*') {
          depth += 1;
          i += 2;
        } else if (text.slice(i, i + 2) === '*/') {
          depth -= 1;
          i += 2;
        } else {
          i += 1;
        }
      }
      masked += ' ';
    } else {
      masked += ch;
      i += 1;
    }
  }
  const names = new Set();
  const re = /\b([A-Za-z][A-Za-z0-9_]*)\s*\(/g;
  let match;
  while ((match = re.exec(masked)) !== null) {
    const name = match[1].toUpperCase();
    if (!keywords.has(name)) {
      names.add(name);
    }
  }
  return [...names].sort();
}

function sigmaFunctionNames(formula) {
  const masked = String(formula || '').replace(/"[^"]*"/g, '""').replace(/\[[^\]]*\]/g, '[]');
  const names = new Set();
  const re = /\b([A-Za-z][A-Za-z0-9_]*)\s*\(/g;
  let match;
  while ((match = re.exec(masked)) !== null) names.add(match[1]);
  return [...names].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

function statusFor({ formula, sigmaFormula, warnings, chart, rls, functionNames, tableIndex }) {
  const rows = functionNames.map((name) => tableIndex.get(name));
  const warningText = warnings.join('\n');
  if (/^\s*\/\*/.test(sigmaFormula) ||
      /(?:NOT|not) converted|no Sigma equivalent|embedded in a larger expression/.test(warningText) ||
      rows.some((row) => row && row.status === 'not_converted')) {
    return 'not_converted';
  }
  if (chart && chart.formula) return 'chart_only';
  if (functionNames.some((name, i) => !rows[i] || rows[i].status === 'unmapped') ||
      /Unmapped Tableau function\(s\)/.test(warningText)) {
    return 'unmapped';
  }
  if (rls) return 'rls';
  if (rows.some((row) => row && row.status === 'chart_only')) {
    // A chart-only function embedded in an unrecognized larger expression was
    // not safely translated even if the bundle did not emit a comment.
    return 'not_converted';
  }
  if (rows.some((row) => row && row.status === 'verify') || /\bverify\b/i.test(warningText)) {
    return 'verify';
  }
  // Function-free arithmetic/IF/CASE formulas are converter-supported.
  return 'spec';
}

let tempDir = null;
try {
  const inputPath = parseArgs(process.argv.slice(2));
  const raw = inputPath ? readFileSync(inputPath, 'utf8') : stdinText();
  if (!raw.trim()) throw new Error('input is empty; expected a JSON formula array');

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`invalid JSON: ${error.message}`);
  }
  const formulas = Array.isArray(parsed) ? parsed : parsed && parsed.formulas;
  if (!Array.isArray(formulas)) throw new Error('JSON must be an array or an object with a formulas array');
  formulas.forEach((entry, index) => {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error(`formulas[${index}] must be an object`);
    }
    if (typeof entry.formula !== 'string') {
      throw new Error(`formulas[${index}].formula must be a string`);
    }
  });

  const table = JSON.parse(readFileSync(TABLE, 'utf8'));
  const tableIndex = new Map(table.functions.map((row) => [row.tableau_fn.toUpperCase(), row]));
  const catalogKeywords = JSON.parse(readFileSync(CATALOG, 'utf8')).keywords || {};
  const keywords = new Set([...(catalogKeywords.logicals || []), ...(catalogKeywords.lod || [])]
    .map((name) => String(name).toUpperCase()));
  if (!keywords.has('IN')) throw new Error(`keyword list missing from ${CATALOG}`);

  tempDir = mkdtempSync(join(tmpdir(), 'tableau-formula-audit-'));
  const tempBundle = join(tempDir, 'tableau-audit.mjs');
  copyFileSync(BUNDLE, tempBundle);
  appendFileSync(tempBundle, SHIM);
  const converter = await import(pathToFileURL(tempBundle).href);
  if (typeof converter.__auditToSigma !== 'function' ||
      typeof converter.__auditWindow !== 'function' ||
      typeof converter.__auditIsRls !== 'function') {
    throw new Error('converter export shim failed; expected formula helpers were not found');
  }

  const results = formulas.map((entry) => {
    const warnings = [];
    const chart = converter.__auditWindow(entry.formula);
    const translated = converter.__auditToSigma(entry.formula, warnings);
    const sigmaFormula = chart && chart.formula ? chart.formula : translated;
    // The bundled IN expansion takes only the last bracket token as its
    // operand, so `[Parameters].[P] IN (...)` comes back as
    // `[Parameters].([P] = ... or ...)` — invalid Sigma. Refuse it rather than
    // pass it as spec.
    if (/\]\s*\.\s*\(/.test(String(sigmaFormula || ''))) {
      warnings.push('⚠ Qualified reference split by the IN expansion — NOT converted: ' +
                    String(sigmaFormula).slice(0, 120));
    }
    const functionNames = tableauFunctionNames(entry.formula, keywords);
    const status = statusFor({
      formula: entry.formula,
      sigmaFormula,
      warnings,
      chart,
      rls: converter.__auditIsRls(entry.formula),
      functionNames,
      tableIndex
    });
    return {
      ...entry,
      status,
      sigma_formula: sigmaFormula,
      warnings,
      function_names: functionNames,
      tableau_functions: functionNames,
      sigma_functions: sigmaFunctionNames(sigmaFormula)
    };
  });

  const counts = Object.fromEntries(STATUSES.map((status) => [status, 0]));
  results.forEach((result) => { counts[result.status] += 1; });
  const converted = results.filter((result) => TRANSLATED.has(result.status)).length;
  process.stdout.write(`${JSON.stringify({
    formulas: results,
    counts,
    converted,
    coverage_pct: results.length === 0 ? 100.0 : Math.round((1000 * converted) / results.length) / 10
  }, null, 2)}\n`);
} catch (error) {
  fail(error && error.message ? error.message : String(error));
} finally {
  if (tempDir) rmSync(tempDir, { recursive: true, force: true });
}
