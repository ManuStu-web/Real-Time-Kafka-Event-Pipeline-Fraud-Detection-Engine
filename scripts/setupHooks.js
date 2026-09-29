const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

function setupHooks() {
  const rootDir = path.resolve(__dirname, '..');
  const gitDir = path.join(rootDir, '.git');
  const githooksDir = path.join(rootDir, '.githooks');

  if (!fs.existsSync(gitDir)) {
    console.warn('[!] Not a git repository. Run "git init" first.');
    return;
  }

  // Set Git core.hooksPath
  try {
    execSync('git config core.hooksPath .githooks', { cwd: rootDir });
    console.log('[+] Configured git core.hooksPath -> .githooks');
  } catch (err) {
    console.warn('[!] Warning configuring core.hooksPath:', err.message);
  }

  // Also copy to .git/hooks/pre-push for redundancy
  const targetHook = path.join(gitDir, 'hooks', 'pre-push');
  const sourceHook = path.join(githooksDir, 'pre-push');

  if (fs.existsSync(sourceHook)) {
    fs.mkdirSync(path.dirname(targetHook), { recursive: true });
    fs.copyFileSync(sourceHook, targetHook);
    try {
      fs.chmodSync(targetHook, '755');
    } catch (e) {
      // Windows file systems might not support POSIX permissions
    }
    console.log(`[+] Installed pre-push hook at ${targetHook}`);
  }

  console.log('[SUCCESS] Pre-push hook active! Any "git push" will automatically execute Jest and block if tests fail.');
}

if (require.main === module) {
  setupHooks();
}

module.exports = { setupHooks };
