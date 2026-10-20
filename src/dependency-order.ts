/** Stable dependency-first traversal. Missing vertices and cycles reject the whole graph. */
export function dependencyOrder<Node>(roots: Iterable<Node>, dependencies: (node: Node) => Iterable<Node | undefined>): Node[] | undefined {
	const states = new Map<Node, boolean>(), ordered: Node[] = [];
	for (const root of roots) {
		const pending: Array<readonly [Node | undefined, boolean]> = [[root, false]];
		while (pending.length) {
			const [node, finished] = pending.pop()!;
			if (node === undefined) return undefined;
			if (finished) { states.set(node, true); ordered.push(node); continue; }
			const state = states.get(node);
			if (state === true) continue;
			if (state === false) return undefined;
			states.set(node, false); pending.push([node, true]);
			const parents = [...dependencies(node)];
			for (let index = parents.length - 1; index >= 0; index--) pending.push([parents[index], false]);
		}
	}
	return ordered;
}
