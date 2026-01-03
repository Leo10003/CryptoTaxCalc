# COLLAB_RULES — FINAL UNIFIED SPEC  
**CryptoTaxCalc Project — Collaboration, Development, and Design Rules**  
**Authoritative Source of Truth | Applies to ALL Workflows**

---

# 1. HARD RULES (OVERRIDE EVERYTHING BELOW)
These rules take absolute priority. No exceptions.

## 1.1 Never Assume File State
- If the assistant is **not 100% certain** about a file's current content → **ask the user to upload it**.
- Never guess. Never approximate. Never “work around” missing context.
- Always work with the **latest file version explicitly provided**.

## 1.2 If Unsure → STOP and Ask
- Any ambiguity, missing snippet, unclear context, or multi-file dependency → ask the user.
- No silent assumptions.

## 1.3 Always Provide Exact Placement Instructions
When providing code:
- Include **the exact line before and after** the insertion point.
- Show **the exact indentation required**.
- Provide **copy-paste-ready** blocks.

## 1.4 psychology-First Visual Decisions
- Visual decisions **must** be backed by human-behavior psychology principles.
- This applies to **PDF layout, UI layout, spacing, icons, color, rhythm, hierarchy**.
- The assistant must explicitly use psychology when producing design/layout decisions.

## 1.5 One-Shot Completeness (When Context Is Guaranteed)
- If all necessary files are present → produce **complete, self-contained responses**.
- No partial solutions.
- This rule yields to **1.1** (Never Assume File State).

## 1.6 Stability & Determinism
- All code must be deterministic.
- No randomness.
- Output must be reproducible across sessions.

---

# 2. WORKFLOW RULES
## 2.1 File-State Discipline
The assistant must:
- Always request the latest file if unsure.
- Never rely on memory of older uploads.
- Treat every task as if code may have changed unless the user explicitly says otherwise.

## 2.2 Task Sequencing
- When requested: provide **up to 4 next steps** ordered by priority.
- Once the user selects a step, complete it before moving to another.

## 2.3 No Divergence
- If a task is started, it must be finished unless the user asks to change direction.

---

# 3. PSYCHOLOGICAL DESIGN RULES
## 3.1 Visual Hierarchy Based on Human Perception
- Top-left = strongest position.
- Large → important. Small → supportive.
- Big spacing between sections = clarity.
- Tiny spacing between related elements = grouping (Gestalt proximity).

## 3.2 Rhythm Rules
- Avoid spacing entropy.
- Use consistent vertical rhythm.
- Every section starts with more space above than below.

## 3.3 Dual Accent Palette (Premium)
- Primary: **#4B9BFF**
- Secondary: **softened version** (#4B9BFF @30–40% opacity or desaturated variant)

## 3.4 Premium-feel Applies ONLY to Visual Components
This rule is explicit from now on:
- **Premium-feel overrides basic functionality ONLY for PDF, UI, icons, layout, visuals.**
- It does *not* override correctness of calculations, API logic, or backend output.

---

# 4. DEMO MODE CONSISTENCY RULE
Demo mode MUST:
- Follow exactly the same visual psychology principles.
- Use the same spacing, icons, palette, hierarchy.
- Avoid visual drift from the real system.

---

# 5. CRITICAL PATH AWARENESS
Whenever a change affects:
- the PDF  
- the dashboard  
- the backend models  
- the runner  
- the exporter  

The assistant must:
1. Warn the user of cross-module impact.  
2. Request all relevant files before touching shared logic.

---

# 6. PDF_STYLE_CONTRACT (NEW — AUTHORITATIVE)
This governs all PDF generation forever.

## 6.1 Margins & Grid
- Fixed outer margin: **36 pt (0.5 inch)**
- Two-column layout discouraged unless explicitly requested.
- Always left-align body text.

## 6.2 Section Headers
Each major section:
- Starts on a **new page** (if requested by user).
- Uses:
  - Icon (44–54 px)
  - Title (14–16 pt, semi-bold)
  - Lead-in sentence (psychology tuned)

## 6.3 Section Rhythm
- 28–38 pt above section header.
- 12–18 pt between header and lead-in.
- 10–16 pt between lead-in and content.

## 6.4 Icon Rules
- Icons ALWAYS:
  - are 512×512 source PNG/SVG
  - scaled to 36–52 px depending on importance
  - aligned precisely with title baseline
  - use #1F1F1F for strokes and #4B9BFF for accents
  - NEVER blurry, never stretched, never pixelated.

## 6.5 Color Rules
- No gradients.
- No shadows.
- No glowing edges.
- Pure flat vector look.
- Blue accent used sparingly for highlights.

## 6.6 Table Rules
- Header shading: 10–14% grey.
- Row padding: 4–6 pt vertically.
- Column spacing: psychologically balanced.
- Never let a table exceed page width.
- At page break:
  - Repeat table header automatically.

## 6.7 Page Break Behavior
- Never break between a section header and its first paragraph.
- Never isolate a section icon alone at the bottom.
- If needed, force a manual break.

---

# 7. ICON SYSTEM RULES
- All icons must follow the EXACT same style contract:
  - 2px stroke
  - Dark-gray outline (#1F1F1F)
  - Blue accent (#4B9BFF)
  - Rounded caps & joins
  - No background
  - No blur, no glow
  - Perfectly centered geometry

- The assistant must regenerate icons with a consistent command when needed.

---

# 8. FRONTEND RULES
## 8.1 No Breaking Styles
- CSS layout changes must respect existing structure.
- If unsure, ask for theme.css, base.html, or related files.

## 8.2 Visual Changes Must Be Psychological
- Every UI decision must state *why* it helps the user’s cognition.

---

# 9. BACKEND RULES
## 9.1 Data Integrity
- Never change database models without asking.
- Never rename fields silently.

## 9.2 API Stability
- New endpoints require explicit user approval.
- Avoid breaking frontend compatibility.

---

# 10. CALCULATION ENGINE RULES
- Must remain deterministic.
- Must match tax logic for Croatia & Italy only (for now).
- Any uncertainty → ask before implementing.

---

# 11. ERROR HANDLING RULES
- Always surface meaningful, actionable errors.
- PDF errors must not crash application; return logged issue.

---

# 12. COLLABORATION RULES
## 12.1 No Guessing
If something is not absolutely known → **ask**.

## 12.2 Clarity First
All instructions, code, and design decisions must be:
- explicit  
- reproducible  
- direct  
- psychology aware  

## 12.3 Strong Alignment
Assistant must maintain perfect awareness of:
- the project identity  
- tone  
- premium design language  
- target users (Croatia & Italy)  

---

# 13. RULE PRECEDENCE (VERY IMPORTANT)
When rules conflict, use this priority order:

1. **Hard Rules (Section 1)**
2. **PDF Style Contract (Section 6)**
3. **Psychology Rules (Section 3)**
4. **Critical Path Awareness (Section 5)**
5. **Backend Stability Rules**
6. **Frontend Rules**
7. **Collaboration Rules**

This guarantees deterministic behavior.

---

# 14. FINAL PRINCIPLE
> **When in doubt: STOP, verify file state, ask user, then produce a complete, psychologically tuned, premium-standard solution.**

