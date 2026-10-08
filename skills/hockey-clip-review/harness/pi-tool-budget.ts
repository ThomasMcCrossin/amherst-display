/**
 * Tool-call budget for pi reviewers (pi has no --max-turns flag).
 *
 * Load with `pi ... -e <skill_dir>/harness/pi-tool-budget.ts` (works with --no-extensions).
 * HCR_MAX_TOOL_CALLS (default 24): after this many tool calls, frame extraction and image
 * reads are blocked with a note to write the verdict; writing it and running
 * check_verdict.py stay allowed. HCR_MAX_TOOL_CALLS + HCR_TOOL_GRACE (default 8) blocks
 * everything and asks pi to shut down, so a looping model cannot run up the plan.
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
	const soft = Number(process.env.HCR_MAX_TOOL_CALLS || 24);
	const hard = soft + Number(process.env.HCR_TOOL_GRACE || 8);
	let calls = 0;

	pi.on("tool_call", async (event, ctx) => {
		calls += 1;
		const text = JSON.stringify(event.input ?? {});
		if (calls > hard) {
			ctx.shutdown();
			return { block: true, reason: `tool budget exhausted (${hard} calls); stop now` };
		}
		if (calls > soft) {
			const finishing = /check_verdict|VERDICT|\.json/.test(text) && !/\.(png|jpe?g)/i.test(text) && !/frames\.py/.test(text);
			if (!finishing) {
				return { block: true, reason: `tool budget spent (${soft} calls): no more frames; write the verdict JSON and run check_verdict.py now` };
			}
		}
		return undefined;
	});
}
