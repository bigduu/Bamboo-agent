/** Keep independent real-Host scenarios observable after an earlier failure. */
export async function runIndependentScenarios(cases, run, report = () => {}) {
  const failures = []
  for (const [label, argument] of cases) {
    try { await run(label, argument) }
    catch (error) {
      failures.push({ label, error: error instanceof Error ? error.stack : String(error) })
      report(label, error)
    }
  }
  return failures
}
