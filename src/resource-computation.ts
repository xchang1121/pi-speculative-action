import { AsyncLocalStorage } from "node:async_hooks";
import { Session } from "node:inspector";
import { hash, randomUUID } from "node:crypto";
import { types } from "node:util";
import { runInNewContext } from "node:vm";
import ts from "typescript";
import type { ResourceReadView, ResourceInputPersistence } from "./resource-version.ts";
import { TimelineInterval } from "./task-timing.ts";

type Context = { readonly view: ResourceReadView; readonly sources: () => Iterable<ResourceReadView>; readonly signal?: AbortSignal };
type ResourceOperationScope = { readonly inputs?: (path?: string) => Iterable<object>; readonly maxBytes: () => number;
	readonly persistentInputs?: ResourceInputPersistence; readonly computation?: Context };
type FunctionNode = ts.FunctionDeclaration | ts.FunctionExpression | ts.ArrowFunction | ts.MethodDeclaration;
type Proof = { readonly binding: object; readonly globals: readonly unknown[]; readonly source: string };
const namespace = "pi-speculative-action.computation/", key = namespace + randomUUID(), access = `globalThis[${JSON.stringify(key)}]`;
const globals: Record<string, unknown> = { Math, JSON, Object, Array, Number, String, Boolean };
const staticCalls: Record<string, ReadonlySet<string>> = {
	Math: new Set("abs acos acosh asin asinh atan atanh atan2 cbrt ceil clz32 cos cosh exp expm1 floor fround hypot imul log log1p log2 log10 max min pow round sign sin sinh sqrt tan tanh trunc".split(" ")),
	JSON: new Set(["parse", "stringify"]), Object: new Set(["keys", "values", "entries"]), Array: new Set(["isArray"]),
	Number: new Set(["isFinite", "isInteger", "isNaN", "isSafeInteger"]),
};
const methods = new Set("at charAt charCodeAt codePointAt concat endsWith every filter find findIndex flat includes indexOf join lastIndexOf map reduce reduceRight slice some split startsWith substring toLowerCase toUpperCase trim trimEnd trimStart".split(" "));
const mutations = new Set(["push", "pop", "shift", "unshift", "reverse", "sort"]);
// Every reachable inherited operation must remain the one audited above, including coercion/iteration.
const intrinsics = [...Object.values(globals), Object.prototype, Array.prototype, String.prototype, Number.prototype, Boolean.prototype,
	Function.prototype, Object.getPrototypeOf([][Symbol.iterator]()), Object.getPrototypeOf(""[Symbol.iterator]()),
	Object.getPrototypeOf(Object.getPrototypeOf([][Symbol.iterator]()))];
const descriptors = intrinsics.map(value => Object.getOwnPropertyDescriptors(value));
const { getOwnPropertyDescriptors, getOwnPropertyDescriptor, is: sameValue } = Object, ownKeys = Reflect.ownKeys;
const prototypes = intrinsics.map(value => Object.getPrototypeOf(value));
// A module loaded after an intrinsic was replaced must not bless that replacement as pure.
const standard = runInNewContext(`({ values: [Math, JSON, Object, Array, Number, String, Boolean,
  Object.prototype, Array.prototype, String.prototype, Number.prototype, Boolean.prototype, Function.prototype,
  Object.getPrototypeOf([][Symbol.iterator]()), Object.getPrototypeOf(""[Symbol.iterator]()),
  Object.getPrototypeOf(Object.getPrototypeOf([][Symbol.iterator]()))], apply: Reflect.apply,
  source: Function.prototype.toString, descriptors: Object.getOwnPropertyDescriptors, prototype: Object.getPrototypeOf })`);
const apply: typeof Reflect.apply = standard.apply;
const matches = (actual: unknown, expected: unknown): boolean => {
	if (typeof actual !== typeof expected || (actual === null) !== (expected === null) ||
		actual !== null && typeof actual === "object" && types.isProxy(actual) || typeof actual === "function" && types.isProxy(actual)) return false;
	if (typeof expected === "function") return apply(standard.source, actual, []) === apply(standard.source, expected, []);
	if (!expected || typeof expected !== "object") return sameValue(actual, expected);
	const index = standard.values.indexOf(expected);
	if (index >= 0) return actual === intrinsics[index];
	return matches(standard.prototype(actual), standard.prototype(expected)) && matchesDescriptors(actual, expected);
};
function matchesDescriptors(actual: unknown, expected: unknown): boolean {
	const left = standard.descriptors(actual), right = standard.descriptors(expected), keys = ownKeys(right);
	return ownKeys(left).length === keys.length && keys.every(key => left[key] &&
		["value", "get", "set", "enumerable", "configurable", "writable"].every(field => matches(left[key][field], right[key][field])));
}
const pristine = intrinsics.every((value, index) => matches(value, standard.values[index]) &&
	matches(prototypes[index], standard.prototype(standard.values[index])) && matchesDescriptors(value, standard.values[index]));
const globalDescriptors = Object.entries(globals).map(([name, value]) => ({ name, value }));
function intact(): boolean {
	if (!pristine) return false;
	for (let i = 0; i < globalDescriptors.length; i++) {
		const { name, value } = globalDescriptors[i]!, current = getOwnPropertyDescriptor(globalThis, name);
		if (!current || !("value" in current) || current.value !== value) return false;
	}
	for (let i = 0; i < intrinsics.length; i++) {
		if (standard.prototype(intrinsics[i]) !== prototypes[i]) return false;
		const current = getOwnPropertyDescriptors(intrinsics[i]), original = descriptors[i]!, keys = ownKeys(original);
		if (ownKeys(current).length !== keys.length) return false;
		for (let j = 0; j < keys.length; j++) {
			const key = keys[j]!, left = current[key as string], right = original[key as string]!;
			if (!left || !sameValue(left.value, right.value) || left.get !== right.get || left.set !== right.set ||
				left.writable !== right.writable || left.configurable !== right.configurable || left.enumerable !== right.enumerable) return false;
		}
	}
	return true;
}

/** Code/proof metadata only. Values, ownership, byte budgets and receipts stay in ResourceReadView. */
function createBroker() {
	const contexts = new AsyncLocalStorage<ResourceOperationScope | undefined>(), proofs = new Map<string, Proof>();
	const parse = JSON.parse, parsing = Object.freeze({});
	return { contexts, proofs, target: undefined as Function | undefined,
		wrap(id: string, original: (...args: unknown[]) => unknown, environment: () => [unknown[], unknown[]]) {
			return (...args: unknown[]) => {
				const context = contexts.getStore()?.computation, proof = proofs.get(id), execute = () => apply(original, undefined, args);
				if (!context || context.signal?.aborted || !proof) return execute();
				const values = TimelineInterval.outsideSync(() => {
					try { if (intact() && apply(standard.source, original, []) === proof.source) { const [values, actualGlobals] = environment();
						if (actualGlobals.every((value, index) => value === proof.globals[index])) return values;
					} } catch { /* A conditional read or unavailable closure keeps its native evaluation order. */ }
				});
				return values ? context.view.compute(proof.binding, [args, values], execute, context.sources) : execute();
			};
		},
		call(receiver: typeof JSON, name: "parse") {
			const original = receiver[name]; // Preserve property access before argument evaluation, including a custom getter.
			return (...args: Parameters<typeof JSON.parse>) => {
				const context = contexts.getStore()?.computation, execute = () => apply(original, receiver, args);
				return context && !context.signal?.aborted && original === parse && typeof args[0] === "string" && args.length === 1 && TimelineInterval.outsideSync(intact)
					? context.view.compute(parsing, args, execute, context.sources) : execute();
			};
		},
	};
}
const broker = createBroker();
Object.defineProperty(globalThis, key, { value: broker });
export const resourceOperationContext = broker.contexts;

const isFunction = (node: ts.Node): node is FunctionNode => ts.isFunctionDeclaration(node) || ts.isFunctionExpression(node) || ts.isArrowFunction(node) || ts.isMethodDeclaration(node);
const contains = (outer: ts.Node, inner: ts.Node) => inner.pos >= outer.pos && inner.end <= outer.end;
const bindingAt = (checker: ts.TypeChecker, node: ts.Node) => ts.isIdentifier(node) && ts.isShorthandPropertyAssignment(node.parent)
	? checker.getShorthandAssignmentValueSymbol(node.parent) : checker.getSymbolAtLocation(node);
const reference = (node: ts.Identifier) => {
	const parent = node.parent;
	return !(ts.isPropertyAccessExpression(parent) && parent.name === node ||
		(ts.isPropertyAssignment(parent) || ts.isMethodDeclaration(parent)) && parent.name === node ||
		(ts.isVariableDeclaration(parent) || ts.isParameter(parent) || isFunction(parent)) && parent.name === node ||
		ts.isBindingElement(parent) && (parent.name === node || parent.propertyName === node) ||
		ts.isBreakStatement(parent) || ts.isContinueStatement(parent) || ts.isLabeledStatement(parent));
};

/** A deliberately closed language: synchronous data calculations and audited intrinsics.
 * Unknown calls, ambient state, input mutation and escaping aliases keep their original execution. */
function prove(fn: FunctionNode, checker: ts.TypeChecker): { captures: string[]; globals: string[]; bindings: Map<string, ts.Symbol | undefined> } | undefined {
	if (!fn.body || fn.asteriskToken || fn.modifiers?.some(value => value.kind === ts.SyntaxKind.AsyncKeyword) ||
		fn.parameters.some(param => !ts.isIdentifier(param.name) || param.initializer || param.dotDotDotToken)) return;
	const locals = new Set<ts.Symbol>(), owned = new Set<ts.Symbol>(), assigned = new Set<ts.Symbol>();
	const captures = new Set<string>(), usedGlobals = new Set<string>();
	const bindings = new Map<string, ts.Symbol | undefined>();
	let valid = true;
	const symbol = (node: ts.Node) => bindingAt(checker, node);
	const write = (node: ts.Node) => {
		if (ts.isIdentifier(node)) { const binding = symbol(node); if (!binding || !locals.has(binding)) valid = false; else assigned.add(binding); }
		else if (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
			const binding = ts.isIdentifier(node.expression) && symbol(node.expression);
			if (!binding || !owned.has(binding)) valid = false;
		} else valid = false;
	};
	const declarations = (node: ts.Node) => {
		if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name) && fn.parameters.some(param => param.name.getText() === node.name.getText())) valid = false;
		if ((ts.isVariableDeclaration(node) || ts.isParameter(node)) && ts.isIdentifier(node.name)) {
			const binding = symbol(node.name); if (binding) {
				locals.add(binding);
				if (ts.isVariableDeclaration(node) && node.initializer && (ts.isArrayLiteralExpression(node.initializer) || ts.isObjectLiteralExpression(node.initializer))) owned.add(binding);
			}
		}
		ts.forEachChild(node, declarations);
	};
	declarations(fn);
	const visit = (node: ts.Node) => {
		if (!valid) return;
		if (node.kind === ts.SyntaxKind.ThisKeyword || node.kind === ts.SyntaxKind.SuperKeyword || ts.isAwaitExpression(node) || ts.isYieldExpression(node) ||
			ts.isNewExpression(node) || ts.isDeleteExpression(node) || ts.isTaggedTemplateExpression(node) || ts.isClassLike(node) || ts.isWithStatement(node) || ts.isMetaProperty(node) ||
			ts.isGetAccessorDeclaration(node) || ts.isSetAccessorDeclaration(node) || ts.isRegularExpressionLiteral(node) || ts.isSpreadAssignment(node) || ts.isSpreadElement(node)) { valid = false; return; }
		if (isFunction(node) && node !== fn && (!ts.isArrowFunction(node) || !ts.isCallExpression(node.parent) ||
			!node.parent.arguments.includes(node) || node.modifiers?.some(value => value.kind === ts.SyntaxKind.AsyncKeyword))) { valid = false; return; }
		if (ts.isIdentifier(node) && reference(node)) {
			if (["arguments", "eval", "Function", "globalThis", "Symbol"].includes(node.text)) { valid = false; return; }
			const binding = symbol(node), local = binding?.declarations?.some(declaration => contains(fn, declaration));
			if (local && binding && !locals.has(binding)) { valid = false; return; }
			if (!local && (node.text !== "undefined" || binding)) {
				if (Object.hasOwn(globals, node.text)) usedGlobals.add(node.text);
				else if (!binding) { valid = false; return; } else captures.add(node.text);
				bindings.set(node.text, binding);
			}
		}
		if (ts.isBinaryExpression(node) && node.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && node.operatorToken.kind <= ts.SyntaxKind.LastAssignment) write(node.left);
		if ((ts.isPrefixUnaryExpression(node) || ts.isPostfixUnaryExpression(node)) && (node.operator === ts.SyntaxKind.PlusPlusToken || node.operator === ts.SyntaxKind.MinusMinusToken)) write(node.operand);
		if ((ts.isForOfStatement(node) || ts.isForInStatement(node)) && !ts.isVariableDeclarationList(node.initializer)) write(node.initializer);
		if (ts.isCallExpression(node)) {
			const call = node.expression;
			if (ts.isIdentifier(call)) {
				if (!["Number", "String", "Boolean"].includes(call.text)) { valid = false; return; }
				usedGlobals.add(call.text);
			} else if (ts.isPropertyAccessExpression(call)) {
				const receiver = call.expression, method = call.name.text;
				if (ts.isIdentifier(receiver) && Object.hasOwn(staticCalls, receiver.text)) {
					if (!staticCalls[receiver.text]!.has(method)) { valid = false; return; }
					usedGlobals.add(receiver.text);
				} else if (mutations.has(method)) {
					const binding = ts.isIdentifier(receiver) && symbol(receiver);
					if (!binding || !owned.has(binding)) { valid = false; return; }
				} else if (!methods.has(method)) { valid = false; return; }
			} else { valid = false; return; }
		}
		ts.forEachChild(node, visit);
	};
	visit(fn.body);
	if ([...owned].some(binding => assigned.has(binding))) valid = false;
	// A global spelling shadowed by any local declaration is not the intrinsic passed at entry.
	if ([...locals].some(binding => usedGlobals.has(binding.name))) valid = false;
	return valid ? { captures: [...captures], globals: [...usedGlobals], bindings } : undefined;
}

type Edit = { readonly start: number; readonly end: number; readonly text: string };
function compile(source: string, id: string) {
	const file = ts.createSourceFile("resource-operation.js", source, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
	const host = ts.createCompilerHost({ allowJs: true, noLib: true, noResolve: true });
	host.getSourceFile = name => name === file.fileName ? file : undefined;
	const checker = ts.createProgram([file.fileName], { allowJs: true, noLib: true, noResolve: true }, host).getTypeChecker();
	const functions: FunctionNode[] = [], edits: Edit[] = [], registered = new Map<string, FunctionNode>(), writes = new Set<ts.Symbol>();
	let dynamic = false;
	const assigned = (node: ts.Node) => {
		if (ts.isIdentifier(node)) { const symbol = bindingAt(checker, node); if (symbol) writes.add(symbol); }
		else if (ts.isArrayLiteralExpression(node) || ts.isObjectLiteralExpression(node)) ts.forEachChild(node, assigned);
		else if (ts.isPropertyAssignment(node)) assigned(node.initializer);
		else if (ts.isShorthandPropertyAssignment(node)) assigned(node.name);
		else if (ts.isParenthesizedExpression(node) || ts.isSpreadElement(node)) assigned(node.expression);
		else if (ts.isBinaryExpression(node)) assigned(node.left);
	};
	const walk = (node: ts.Node) => {
		if (isFunction(node)) functions.push(node);
		if (ts.isIdentifier(node) && ["eval", "globalThis"].includes(node.text) &&
			(node.text === "eval" || !!bindingAt(checker, node))) dynamic = true;
		if ((ts.isPropertyAccessExpression(node) && ["toString", "caller", "callee", "stack"].includes(node.name.text)) ||
			ts.isElementAccessExpression(node) && ts.isStringLiteral(node.argumentExpression) && ["toString", "caller", "callee", "stack"].includes(node.argumentExpression.text)) dynamic = true;
		if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name) && (bindingAt(checker, node.name)?.declarations?.length ?? 0) > 1) assigned(node.name);
		if (ts.isBinaryExpression(node) && node.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && node.operatorToken.kind <= ts.SyntaxKind.LastAssignment) assigned(node.left);
		if ((ts.isPrefixUnaryExpression(node) || ts.isPostfixUnaryExpression(node)) && (node.operator === ts.SyntaxKind.PlusPlusToken || node.operator === ts.SyntaxKind.MinusMinusToken)) assigned(node.operand);
		if ((ts.isForOfStatement(node) || ts.isForInStatement(node)) && !ts.isVariableDeclarationList(node.initializer)) assigned(node.initializer);
		ts.forEachChild(node, walk);
	}; walk(file);
	if (dynamic) return;
	const pure = new Map<FunctionNode, { identity: string; proof: NonNullable<ReturnType<typeof prove>> }>();
	for (const fn of functions) {
		if (!ts.isExternalModule(file) && !functions.some(parent => parent !== fn && contains(parent, fn))) continue;
		const proof = prove(fn, checker); if (!proof) continue;
		pure.set(fn, { identity: `${id}:${fn.pos}`, proof });
	}
	const parse = (node: ts.Node, inside = false) => {
		inside ||= isFunction(node);
		if (inside && ts.isCallExpression(node) && !(node.flags & ts.NodeFlags.OptionalChain) && ts.isPropertyAccessExpression(node.expression) && ts.isIdentifier(node.expression.expression) &&
			node.expression.expression.text === "JSON" && node.expression.name.text === "parse") {
			edits.push({ start: node.expression.getStart(file), end: node.expression.end, text: `${access}.call(JSON,"parse")` });
		} else if (inside && ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
			const binding = bindingAt(checker, node.expression), declaration = binding?.declarations?.[0];
			const fn = declaration && (isFunction(declaration) ? declaration : ts.isVariableDeclaration(declaration) && declaration.initializer && isFunction(declaration.initializer) ? declaration.initializer : undefined);
			const operation = fn && pure.get(fn);
			if (operation && binding && !writes.has(binding) && [...operation.proof.bindings].every(([name, symbol]) => checker.resolveName(name, node, ts.SymbolFlags.Value, false) === symbol)) {
				const { identity, proof } = operation;
				edits.push({ start: node.expression.getStart(file), end: node.expression.end,
					text: `${access}.wrap(${JSON.stringify(identity)},${node.expression.text},()=>[[${proof.captures.join(",")}],[${proof.globals.join(",")}]] )` });
				registered.set(identity, fn!);
			}
		}
		ts.forEachChild(node, child => parse(child, inside));
	}; parse(file);
	if (!edits.length) return;
	edits.sort((a, b) => b.start - a.start);
	const edited = (start: number, end: number) => {
		let result = source.slice(start, end);
		for (const edit of edits) if (edit.start >= start && edit.end <= end)
			result = result.slice(0, edit.start - start) + edit.text + result.slice(edit.end - start);
		return result;
	};
	for (const [identity, fn] of registered) {
		const start = ts.isFunctionDeclaration(fn) ? fn.getChildren(file).find(node => node.kind === ts.SyntaxKind.FunctionKeyword)!.getStart(file) : fn.getStart(file);
		broker.proofs.set(identity, { binding: Object.freeze({}), globals: pure.get(fn)!.proof.globals.map(name => globals[name]), source: edited(start, fn.end) });
	}
	return { source: edited(0, source.length), registered: [...registered.keys()] };
}

type Script = { readonly source: string; readonly compiled: string; readonly registered: readonly string[]; readonly owners: Set<object> };
const scripts = new Map<string, Script>();
let lane = Promise.resolve();
const serial = <Value>(execute: () => Promise<Value>): Promise<Value> => { const result = lane.then(execute); lane = result.then(() => {}, () => {}); return result; };

/** Attach to the existing function's script without copying its closure or executing its outer tool ahead. */
export function resourceComputationCompiler() {
	const owner = {}, seen = new WeakSet<Function>(); let closed = false;
	const inspect = async <Value>(execute: (post: <Result>(method: string, params?: object) => Promise<Result>) => Promise<Value>) => {
		const session = new Session(); session.connect();
		const post = <Result>(method: string, params: object = {}) => new Promise<Result>((resolve, reject) =>
			session.post(method, params, (error, result) => error ? reject(error) : resolve(result as Result)));
		try { await post("Debugger.enable"); return await execute(post); }
		finally { broker.target = undefined; await post("Debugger.disable").catch(() => {}); session.disconnect(); }
	};
	return {
		prepare: (callback: Function) => serial(async () => {
			if (closed || seen.has(callback) || !intact()) return;
			try { await inspect(async post => {
				broker.target = callback;
				const group = randomUUID();
				try {
					const value = await post<{ result: { objectId?: string } }>("Runtime.evaluate", { expression: `${access}.target`, objectGroup: group });
					if (!value.result.objectId) { seen.add(callback); return; }
					const details = await post<{ internalProperties?: { name: string; value?: { value?: { scriptId: string; lineNumber: number; columnNumber: number } } }[] }>("Runtime.getProperties", { objectId: value.result.objectId, ownProperties: true });
					const location = details.internalProperties?.find(property => property.name === "[[FunctionLocation]]")?.value?.value;
					if (!location) { seen.add(callback); return; }
					const previous = scripts.get(location.scriptId);
					if (previous) { previous.owners.add(owner); seen.add(callback); return; }
					const { scriptSource: source } = await post<{ scriptSource: string }>("Debugger.getScriptSource", { scriptId: location.scriptId });
					if (source.length > 2 * 1024 * 1024 || source.includes(namespace)) { seen.add(callback); return; }
					const compiled = compile(source, `${hash("sha256", source)}:${location.scriptId}`); if (!compiled) { seen.add(callback); return; }
					const edited = await post<{ status?: string; exceptionDetails?: unknown }>("Debugger.setScriptSource", { scriptId: location.scriptId, scriptSource: compiled.source });
					if (edited.exceptionDetails || edited.status && edited.status !== "Ok") { for (const id of compiled.registered) broker.proofs.delete(id); return; }
					scripts.set(location.scriptId, { source, compiled: compiled.source, registered: compiled.registered, owners: new Set([owner]) });
					seen.add(callback);
				} finally { await post("Runtime.releaseObjectGroup", { objectGroup: group }); }
			}); } catch { /* Unsupported live editing retains the original Actor behavior. */ }
		}),
		dispose: () => serial(async () => {
			closed = true;
			for (const [scriptId, script] of scripts) {
				script.owners.delete(owner); if (script.owners.size) continue;
				try { await inspect(async post => {
					const current = await post<{ scriptSource: string }>("Debugger.getScriptSource", { scriptId });
					if (current.scriptSource === script.compiled) {
						const restored = await post<{ status?: string; exceptionDetails?: unknown }>("Debugger.setScriptSource", { scriptId, scriptSource: script.source });
						if (restored.exceptionDetails || restored.status && restored.status !== "Ok") return;
					}
					scripts.delete(scriptId); for (const id of script.registered) broker.proofs.delete(id);
				}); } catch { /* An active frame may postpone restoration; the no-context thunk remains authoritative. */ }
			}
		}),
	};
}
