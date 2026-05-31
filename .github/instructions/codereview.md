# Code Review Instructions for AI Assistants

This instructions guides AI assistants through performing thorough code reviews on attached .diff file and learning from review feedback.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
   - [Required Knowledge](#required-knowledge)
   - [Workspace Scope Restrictions](#-critical-workspace-scope-restrictions)
3. [Code Review Process](#code-review-process)
4. [Review the Code](#review-the-code)
5. [Review Guidelines](#review-guidelines)
6. [Learning from Feedback](#learning-from-feedback)
7. [Updating codereview_learnings.md](#updating-learningsmd)
8. [Best Practices Summary](#best-practices-summary)
9. [Typical AI Review Workflow](#typical-ai-review-workflow)


## Overview

Code reviews are a critical part of the development process. As an AI assistant, you should:
- Provide thorough, constructive feedback on code changes
- Follow established coding standards and best practices
- Learn from corrections to improve future reviews
- Document learnings to maintain consistency

### ⚠️ CRITICAL: Avoid Verbosity

DO NOT include in reviews:
- ❌ Context re-caps on every commit
- ❌ "Strengths" or "What Went Well" sections
- ❌ Lists of "good practices" or "positive findings"
- ❌ Generic praise ("great work!", "well done!")

DO include in reviews:
- ✅ Specific issues with inline comments
- ✅ Concerns that need addressing
- ✅ Blocking problems
- ✅ Questions for clarification

**If there are no issues, simply APPROVE without commentary.**

## Prerequisites

### Required Knowledge
User should provide you with the following information in implementation.md to perform an effective review in the following format:
```markdown
## Implementation Context
**Type of change** 
[New feature / Enhancement / Bug fix / Other]

**Description**
[Brief description of the change]

**Requirements**
[List of requirements or acceptance criteria (if applicable)]

**Implementation details**
[Any specific implementation details or design decisions to be aware of]

**External dependencies**
[Any new libraries or tools used (if applicable)]

```

### 🚨 CRITICAL: Workspace Scope Restrictions

**NEVER access directories outside the workspace root.**

When reviewing code, you have LIMITED permissions:
- ✅ **ALLOWED**: `omashu/` and subdirectories
- ❌ **FORBIDDEN**: System directories (e.g., `/tmp/`, `/var/`, `/etc/`)
- ❌ **FORBIDDEN**: User config directories (e.g., `~/.config/`, `~/.ssh/`)


**What this means for code reviews:**
- Use ONLY files within the repository workspace
- All changed files in diff file are within the workspace - review them directly
- Do NOT attempt to read or write files outside `omashu/`
- Do NOT attempt to write temporary files outside the workspace
- **CRITICAL**: Do NOT use `/dev/null`, `/dev/stdin`, `/dev/stdout`, or any `/dev/` paths
  - These are system devices outside the workspace
  - Use `.aitmp/` files or shell patterns like `|| true` instead

**Temporary Files:**
- ✅ Use `.aitmp/` directory within workspace for any temporary files
- ✅ Create with: `mkdir -p .aitmp`
- ❌ Do NOT use `/tmp/` or other system directories

**Shell Command Restrictions:**
- ❌ **NEVER use `/dev/null`** in redirects - use `.aitmp/output.txt` or omit output instead
- ❌ **NEVER use `/dev/stdin`, `/dev/stdout`, `/dev/stderr`** - use pipes or workspace files
- ❌ **NEVER use system paths** like `/var/`, `/etc/`, `/usr/`, `/bin/`, `/home/`
- ✅ Use workspace-relative paths only: `.aitmp/`, `./`, relative paths
- ✅ If you need to discard output: pipe to `grep . || true` or redirect to `.aitmp/discard.txt`
- ✅ If checking command success: use `|| true` or capture to workspace file

**Examples of FORBIDDEN commands:**
```bash
# ❌ WRONG - triggers permission error
cat file.json | jq '.field' 2>/dev/null
grep pattern file 2>&1 > /dev/null

# ✅ CORRECT - stays in workspace
cat file.json | jq '.field' 2>.aitmp/jq_errors.txt
grep pattern file > .aitmp/grep_output.txt 2>&1
cat file.json | jq '.field' 2>&1 | grep . || true
```

### 🚨 CRITICAL: Do NOT Run Tests or Builds

**The code review workflow is for static review ONLY — do not run `go test`, `go build`, or any compilation/test commands.**

- ❌ **NEVER** run `go test ./...` or any variant
- ❌ **NEVER** run `go build` or `go vet`
- ❌ **NEVER** run any test suite, linter, or compilation tool
- ✅ **DO** review code by reading files and diffs
- ✅ **DO** use `grep`, `git diff`, and file reading to analyze code

**Why:** The review should focus on reading and analyzing code, not executing it.

## Code Review Process

### Step 1: Gather PR Context

**What to look for**:
- Changed files and their purpose
- Commit messages and history
- **CRITICAL**: Check if you've reviewed this PR before by reading user provided review.md
  - If yes: This is an incremental review - only look at NEW changes
  - If no: This is an initial review - review everything

### Step 2: Analyze Implementation/Requirement Context

**Note**: Only do this on INITIAL review, not on every incremental review.

Read user provided implementation.md. If implementaion is any of below categories

**New feature**

Check whether user is implemented feature from scratch or using some opensource library
- If used opensource library then understand which library used and then analyze its suitable for our requirements
- Check whether end-to-end implementation is added
- If end-to-end functionalities are not added then suggest missing pieces
- If can't implement missing pieces in current scope then ask user to add todo with priority like "TODO: P0: [Short description of missing piece]"

**Enhancements**
- Analyze what's existing implementation and what are the new enhancement added
- Check whether the new changes are breaking any current features

**Bug Fix**
- Root cause of the bug being fixed
- Test criteria and acceptance requirements
- Expected behavior vs. actual behavior

**Others**

Understand requirement and make sure added changes will accomplish requirements.

Then review code by following below guildlines.

## Review the Code

### Logic and Design

- ✅ **Correctness**:
  - Does the fix address the root cause?
  - Are edge cases handled?
  - Is the logic sound and complete?

- 🏗️ **Architecture**:
  - Follows established patterns?
  - Maintains separation of concerns?
  - Appropriate abstraction level?

- 📊 **Performance**:
  - Efficient algorithms and data structures
  - Reasonable resource usage
  - Potential bottlenecks

### General

#### Readability & Simplicity
- Prefer simple, clear code over clever tricks.
- Follow philosophy: “clear is better than clever.”
- Avoid unnecessary abstraction.
- Use short, meaningful names.
- Follow DRY (Don’t Repeat Yourself)
- Extract reusable logic

#### Performance & Efficiency
- Avoid unnecessary computations or loops.
- Consider time and space complexity.
- Avoid premature optimization.
- Use lazy evaluation when appropriate.
- Cache results of expensive operations if reused.
- Avoid blocking operations in critical paths.

#### Code Quality

- 🔧 **Maintainability**:
  - Use Design Patterns appropriately.
  - Remove dead code and commented-out sections.
  - Avoid large functions; break into smaller ones.
  - Follow consistent formatting and style.
  - Complexity management
  - Code duplication
  - Future extensibility

- 🧪 **Testability**:
  - Unit test coverage
  - Test quality and clarity
  - Edge case testing

#### Safety and Correctness

- ⚠️ **Critical Issues**:
  - Memory leaks or resource leaks
  - SQL injection or security vulnerabilities
  - Data corruption risks
  - Never hardcode secrets
  - Validate inputs

- 🛡️ **Error Handling**:
  - Error propagation and wrapping
  - Graceful degradation
  - User-facing error messages

#### Comments & Documentation
- Comments should explain "why", not "what".
- Avoid redundant comments that restate code.
- Ensure public functions have docstrings.
- Check for accurate and up-to-date documentation.

**Note**: If there are no documentation, then give documentation suggestions in the review comments. If there are documentation but not up to the mark then suggest improvements in the review comments.

### Go lang Specific

- Write zero allocation code when possible
- Race conditions and data races
- Mutex usage and lock ordering
- Goroutine lifecycle management
- Channel usage patterns
- Shared state access
- Goroutines don’t leak


## Review Guidelines

### Verbosity Control

**DO NOT include in your reviews:**
- ❌ "Strengths" or "Positive Findings" sections listing good code
- ❌ Generic praise like "good job", "well done", "nice work"
- ❌ Repetitive summaries on every new commit
- ❌ Long explanations of the bug or story being fixed

**DO include in your reviews:**
- ✅ Specific issues, concerns, or blocking problems
- ✅ Actionable suggestions with code examples
- ✅ Questions seeking clarification
- ✅ Brief rationale for why an issue matters

**Review Philosophy:**
- If code is good, place a comment that you approve
- Only comment on things that need to change or clarification
- Be surgical - comment on the specific line/block with the issue
- Keep each comment focused on one issue

### Comment Tone and Style

**Use Clear Prefixes**:
- `Nit:` - Minor suggestions, not blocking
- `Concern:` - Important issues that need attention
- `Blocking:` - Critical issues that must be fixed
- `Question:` - Seeking clarification

**Examples**:
```
✅ Good:
"Nit: Consider using a more descriptive variable name here. `un` could be `userName` for clarity."

"⚠️ Concern: Possible data race - goroutine reads `x` without acquiring the lock."

"🚨 Blocking: This will panic if `response` is nil. Add a nil check before dereferencing."

❌ Avoid:
"This is wrong."
"Bad code."
"Why did you do this?"
"Great implementation! The code follows best practices..."
```

**Be Constructive**:
- Explain WHY something matters
- Suggest specific fixes
- Provide code examples when helpful
- Ask questions when uncertain

### Emoji Guide

Use emojis to make reviews scannable:
- ✅ Positive findings / Approved
- ⚠️ Warnings / Concerns
- ❌ Blocking issues
- 🔍 Needs investigation
- 💡 Suggestions / Ideas
- 🛡️ Security related
- 🚨 Critical issues
- 🔒 Thread safety
- 📚 Documentation
- 🧪 Testing


### When to Approve vs. Request Changes

**APPROVE** when:
- Code is safe and correct
- Only minor nits remain (or no issues at all)
- Design is sound
- Tests are adequate
- **Note**: Can approve WITHOUT any comments if there are no issues

**REQUEST_CHANGES** when:
- Critical safety issues exist
- Logic is flawed
- Major architectural concerns
- Insufficient error handling
- Missing required tests

**COMMENT** when:
- Just asking questions
- Providing suggestions
- Not ready for final decision

**IMPORTANT**: 
- On subsequent commits, only review the NEW changes unless asked otherwise
- Do not re-post the same feedback on every commit
- Do not re-summarize each time


## Learning from Feedback

### When You Receive Corrections

If a developer or reviewer corrects your review:

1. **Acknowledge the correction** gracefully
2. **Understand the reasoning** - Why was your assessment wrong?
3. **Update your mental model** - What domain knowledge was missing?
4. **Document the learning** - Add to codereview_learnings.md

### Common Correction Scenarios

#### Scenario 1: Flagged a Non-Issue
```
Your comment: "This will cause a memory leak"
Correction: "Actually, the defer statement handles cleanup properly"
```

**What to learn**:
- How defer works in this context
- When cleanup is automatic vs. manual
- The specific pattern being used

#### Scenario 2: Missed a Real Issue
```
Developer: "Good catch on the nil check, but you missed the race condition on line 45"
```

**What to learn**:
- What patterns indicate race conditions
- How to spot concurrent access issues
- This specific code pattern to watch for

#### Scenario 3: Wrong Severity
```
Your comment: "Blocking: Variable name is unclear"
Correction: "This is a nit, not blocking. The logic is correct."
```

**What to learn**:
- What constitutes blocking vs. nit
- When to prioritize readability vs. correctness
- Team's coding standards


## Updating codereview_learnings.md

### When to Update

Update `codereview_learnings.md` when you:
- Are corrected on a code review
- Learn a new pattern or idiom
- Discover a project-specific convention
- Understand a domain-specific behavior
- Learn from repeated mistakes

### How to Update

The `.github/instructions/codereview_learnings.md` file should be organized by topic. Add entries in this format:

```markdown
## [Topic Category]

### [Specific Learning Title] (Date: YYYY-MM-DD)

**Context**: Brief description of the situation where this was learned

**What I learned**: Clear explanation of the learning

**Why it matters**: Why this is important for future reviews

**How to apply**: Specific guidance for future reviews

**Example**:
\`\`\`go
// Code example if applicable
\`\`\`

```

### Example Entries

```markdown
## Thread Safety

### Read-Modify-Write Requires Locking Even for "Simple" Operations (Date: 2025-11-21)

**Context**: Missed a race condition on a counter increment

**What I learned**: Even simple operations like `counter++` are not atomic in Go 
and require mutex protection when accessed by multiple goroutines.

**Why it matters**: Subtle race conditions can cause data corruption and are 
hard to debug in production.

**How to apply**: Always flag read-modify-write operations on shared state:
- `counter++` / `counter--`
- `map[key] = value` (map access itself)
- Append to slices
- Any compound operation

Unless protected by:
- sync.Mutex
- sync.RWMutex
- atomic operations (sync/atomic package)
- Channel communication

**Example**:
\`\`\`go
// ❌ Race condition
var counter int
go func() { counter++ }()
go func() { counter++ }()

// ✅ Properly synchronized
var counter int
var mu sync.Mutex
go func() { mu.Lock(); counter++; mu.Unlock() }()
go func() { mu.Lock(); counter++; mu.Unlock() }()

// ✅ Using atomic
var counter int64
go func() { atomic.AddInt64(&counter, 1) }()
go func() { atomic.AddInt64(&counter, 1) }()
\`\`\`

---

## Project-Specific Patterns

### Translation Placeholder Strings Are Intentional (Date: 2025-11-21)

**Context**: Flagged "nonsense" translation strings as errors

**What I learned**: When creating resource bundles, the team intentionally uses 
placeholder/gibberish translations like "XYZZY_ALERT_TITLE" to signal to 
translators which strings need attention. This is NOT a bug.

**Why it matters**: Prevents false positive reports and helps understand the 
localization workflow.

**How to apply**: 
- Don't flag unusual translation strings as errors
- Check if there's a pattern (all caps, specific prefix, etc.)
- Verify placeholder strategy follows team conventions
- Only flag if English strings are also nonsensical

```

### Categories for codereview_learnings.md

Organize learnings into these categories:

1. **Go Language Patterns**
   - Language features and idioms
   - Standard library usage
   - Common patterns

2. **Thread Safety and Concurrency**
   - Goroutines and channels
   - Mutex usage
   - Race conditions
   - Atomic operations

3. **Error Handling**
   - Error checking patterns
   - Error wrapping
   - Recovery strategies

4. **Project-Specific Patterns**
   - Team conventions
   - Architectural decisions
   - Domain-specific logic

5. **Security and Safety**
   - Authentication/authorization
   - Input validation
   - Resource management

6. **Testing Patterns**
   - Test structure
   - Mocking strategies
   - Coverage expectations

7. **Performance Considerations**
   - Optimization patterns
   - Profiling insights
   - Scalability concerns

8. **API Design**
   - REST conventions
   - Request/response patterns
   - Versioning strategies

### Updating Process

When adding a new learning:

1. **Create .aitmp directory** if it doesn't exist
2. **Read current codereview_learnings.md** to avoid duplicates
3. **Find appropriate category** or create new one
4. **Add entry with complete information**
5. **Use consistent formatting**
6. **Include date references**

```bash
# Example workflow
mkdir -p .aitmp

# Read current learnings
cat .github/instructions/codereview_learnings.md > .aitmp/current_codereview_learnings.md

# Edit .github/instructions/codereview_learnings.md with new entry
# ... add your learning ...

```

## Best Practices Summary

### Do's ✅
- ✅ Analyze requirements context when available (but don't re-summarize it)
- ✅ Check for existing threads before commenting
- ✅ Use clear prefixes (Nit, Concern, Blocking)
- ✅ Provide specific, actionable feedback ONLY
- ✅ Include code examples in suggestions
- ✅ Learn from corrections and update codereview_learnings.md
- ✅ Approve without comments if code is good
- ✅ **Stay within workspace scope** (`omashu/`)

### Don'ts ❌
- ❌ Re-summarize
- ❌ Add "Strengths" or "Positive Findings" sections
- ❌ Praise code that works correctly
- ❌ Make assumptions without verification
- ❌ Flag issues without explaining impact
- ❌ Approve with unresolved safety concerns
- ❌ Skip bug/story analysis
- ❌ Use harsh or unclear language
- ❌ Ignore project-specific conventions
- ❌ Forget to document learnings
- ❌ Leave temporary files in workspace
- ❌ Put large summaries in single comments
- ❌ Review old code on subsequent commits (only review new changes)
- ❌ **Access directories outside workspace**
- ❌ **Use `/tmp/` or system directories** (use `.aitmp/` instead)
- ❌ **Use `/dev/null` or any `/dev/` paths in commands** (use workspace files or `|| true`)
- ❌ **Read user home directory or parent paths** (stay in repo only)

## Checklist

### Initial Review Checklist

Before submitting your FIRST review:

- [ ] Read Requirements and Bug description (if provided)
- [ ] Analyzed bug/story context (if applicable)
- [ ] Reviewed all changed files
- [ ] Checked for safety issues (nil pointers, races, leaks)
- [ ] Verified error handling
- [ ] Assessed design and architecture
- [ ] Added inline comments with clear feedback (issues only, no praise)
- [ ] Used appropriate comment prefixes
- [ ] Identified review as AI-generated
- [ ] **Do NOT add summary comment or "strengths" section**

### Incremental Review Checklist

Before submitting SUBSEQUENT reviews after new commits:

- [ ] Checked what changed since last review
- [ ] Reviewed ONLY the new changes
- [ ] Added inline comments for new issues only
- [ ] **RESOLVED threads for issues that were fixed** ← MANDATORY
- [ ] **Did NOT leave threads unresolved without documented reason**
- [ ] **Did NOT re-summarize the PR**
- [ ] **Did NOT re-review old code**
- [ ] **Did NOT re-post old feedback**
- [ ] **Did NOT add "positive findings" section**
- [ ] If no new issues: Just APPROVE without comments
- [ ] Updated codereview_learnings.md if corrections received

---


## Typical AI Review Workflow

- Review the code based on guildlines mentioned above and give review comments
- Save the review comments in .aitemp/review.md file
- If user provided review.md file then its incremental review so only review the new changes and give comments accordingly


### Saving review.md

Update `.aitemp/review.md` as shown in format below:

```markdown
## Date: YYYY-MM-DD

### Feature Enhancement: [Short Description of Enhancement]

### Comments:
[Comments list]

### Report:
[Should include what's the current implementation, what's the new implementation, and how much it's improving code quality/functionality/performance etc.]

### **✅ FINAL VERDICT**
[Approved / Request Changes]

**Confidence Level**: **[Percentage]**

**Reasoning:**
[Reasons]

**📝 FINAL CHECKLIST FOR GO-LIVE**
[Final check items before production deployment, if applicable]

```

**Example FINAL VERDICT** 
```markdown
## **🎉 APPROVED FOR PRODUCTION DEPLOYMENT** 🎉

**Confidence Level**: **98%** (pending integration tests)

**Reasoning:**
- ✅ All critical issues resolved
- ✅ All major issues addressed
- ✅ Robust error handling
- ✅ Excellent transaction safety
- ✅ AWS-compliant implementation
- ✅ Production-grade code quality
- ✅ Defensive programming practices
- ✅ Optimal performance characteristics

**Only remaining item**: **Integration & load testing** (which is standard practice before any production deployment)

---

**📝 FINAL CHECKLIST FOR GO-LIVE**

[ ] Write integration tests (USER, GROUP, POLICY CRUD + relationships)
[ ] Load test with 10,000 policies, 1,000 users, 100 groups
[ ] Concurrent operation testing (100 goroutines)
[ ] Deploy to staging environment
[ ] Run smoke tests on staging
[ ] Monitor metrics for 24 hours on staging
[ ] Security review/penetration testing
[ ] Create runbook for common issues
[ ] Set up alerts for error rates > 1%
[ ] Deploy to production (blue-green deployment)
[ ] Monitor closely for 48 hours
[ ] 🎉 Celebrate successful launch!
```
