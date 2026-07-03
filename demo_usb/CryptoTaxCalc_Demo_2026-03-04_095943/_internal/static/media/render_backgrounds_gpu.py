"""
GPU renderer for CryptoTaxCalc animated backgrounds.

- Uses moderngl to animate a single PNG (light or dark theme).
- Mirrors the psychology rules from the CPU version:
  - calm, dual-layer flow
  - edge-safe warp (no border artifacts)
  - breathing brightness
  - soft lighting sweep
  - tiny chromatic drift
"""

import math
import os
from dataclasses import dataclass

import imageio
import moderngl
import numpy as np
from PIL import Image


@dataclass
class RenderConfig:
    src_path: str
    out_path: str
    width: int = 2048
    height: int = 1152
    seconds: float = 60.0
    fps: int = 30
    amp: float = 0.001
    is_dark: bool = True
    crf: int = 0
    # Global motion strength multiplier (1.0 = base, 0.8 = calmer, 1.2 = more alive)
    motion_level: float = 0.5
    # Safe UI zone in UV coords (xmin, ymin, xmax, ymax)
    safe_rect: tuple = (0.20, 0.22, 0.80, 0.78)


VERT_SHADER = """
#version 330

in vec2 in_pos;
in vec2 in_uv;
out vec2 v_uv;

void main() {
    v_uv = in_uv;
    gl_Position = vec4(in_pos, 0.0, 1.0);
}
"""


FRAG_SHADER = """
#version 330

uniform sampler2D u_tex;
uniform float u_time;        // seconds
uniform float u_amp;         // base displacement
uniform int   u_is_dark;
uniform float u_loop;        // loop length in seconds
uniform vec4  u_safeRect;    // xy = min UV, zw = max UV of calm content region

in vec2 v_uv;
out vec4 f_color;

// --------- hash + noise + fbm (simple but good) ---------
float hash(vec2 p){
    return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453123);
}

float noise(vec2 p){
    vec2 i = floor(p);
    vec2 f = fract(p);
    float a = hash(i);
    float b = hash(i + vec2(1.0, 0.0));
    float c = hash(i + vec2(0.0, 1.0));
    float d = hash(i + vec2(1.0, 1.0));
    vec2 u = f*f*(3.0 - 2.0*f);
    return mix(a, b, u.x) + (c - a)*u.y*(1.0 - u.x) + (d - b)*u.x*u.y;
}

// Slightly reduced octaves to avoid shimmer
float fbm(vec2 p){
    float v = 0.0;
    float a = 0.5;
    for(int i = 0; i < 4; ++i){
        v += a * noise(p);
        p *= 2.0;
        a *= 0.5;
    }
    return v;
}

// Smoothstep helper
float smooth3(float x){
    return x * x * (3.0 - 2.0 * x);
}

void main(){
    vec2 uv = v_uv;              // 0..1
    vec2 U = uv;                 // same as CPU version names
    vec2 p = uv * 2.0 - 1.0;     // -1..1, centered

    // ---------- Time + loop phase ----------
    float t = u_time;
    float phase = t / max(u_loop, 0.001);          // 0..1 over one full loop
    float loopT = 2.0 * 3.14159265 * phase;       // 0..2π over loop

    // ---------- Morphing noise fields (no global sliding) ----------
    // Two animated blends, both perfectly looping (integer cycles).
    // We add smooth, position-dependent phase offsets so different
    // regions of the image breathe slightly out of sync.

    // Local phase fields (smooth across the image).
    float region_phase_fast = fbm(U * 0.8 + vec2(5.3, -2.1));   // 0..1
    float region_phase_slow = fbm(U * 0.5 + vec2(-3.7, 4.9));   // 0..1

    // Convert to radians (moderate offsets to avoid "turbulent" feel).
    float phase_off_fast = (region_phase_fast - 0.5) * 1.6;     // ≈ ±1.6 rad
    float phase_off_slow = (region_phase_slow - 0.5) * 1.2 + 1.3; // keep base 1.3

    // 4 morphs per loop – main motion (more visible)
    float blend_fast = 0.5 + 0.5 * sin(loopT * 4.0 + phase_off_fast);

    // 6 morphs per loop – secondary, still seamless
    float blend_slow = 0.5 + 0.5 * sin(loopT * 6.0 + phase_off_slow);

    // Main layer (medium scale)
    vec2 s1 = U * 0.9;
    vec2 s2 = U * 0.6;

    float n1A = fbm(s1 + vec2(0.0, 0.0));
    float n1B = fbm(s1 + vec2(12.7, -8.4));
    float n2A = fbm(s2 + vec2(-4.3, 7.1));
    float n2B = fbm(s2 + vec2(9.6, 3.2));

    float n1 = mix(n1A, n1B, blend_fast);
    float n2 = mix(n2A, n2B, blend_fast);

    // Slow layer (larger scale, slower morph)
    vec2 s1s = U * 0.5;
    vec2 s2s = U * 0.35;

    float n1sA = fbm(s1s + vec2(2.4, -5.7));
    float n1sB = fbm(s1s + vec2(-11.3, 6.2));
    float n2sA = fbm(s2s + vec2(3.9, 4.4));
    float n2sB = fbm(s2s + vec2(-7.8, -9.0));

    float n1s = mix(n1sA, n1sB, blend_slow);
    float n2s = mix(n2sA, n2sB, blend_slow);

    // ---------- Edge mask (no warp near borders) ----------
    float edgeU = min(U.x, 1.0 - U.x);
    float edgeV = min(U.y, 1.0 - U.y);
    float edgeDist = min(edgeU, edgeV);
    float edge_mask = clamp((edgeDist - 0.06) / (0.16 - 0.06), 0.0, 1.0);
    edge_mask = smooth3(edge_mask);

    // ---------- Attentional bias ----------
    // We no longer bias the motion itself; keep displacement neutral.
    float bias = 1.0;

    // ---------- Safe UI zone (calmer motion where content lives) ----------
    vec2 safeCenter = 0.5 * (u_safeRect.xy + u_safeRect.zw);
    vec2 safeHalf   = 0.5 * (u_safeRect.zw - u_safeRect.xy);
    safeHalf = max(safeHalf, vec2(0.0001)); // avoid division by zero

    vec2 d = abs(U - safeCenter) / safeHalf;  // 0 at center, >=1 outside
    float box = 1.0 - clamp(max(d.x, d.y), 0.0, 1.0); // 1 inside, 0 outside
    float safe = smooth3(box);                          // soft interior
    float safe_factor = mix(0.25, 1.0, 1.0 - safe);     // 0.25 inside → 1.0 outside

    float motion_mask = edge_mask * safe_factor;

    // ---------- Displacement from two layers (local directions, smoothed & normalized) ----------
    // ---------- Displacement from two layers (local directions, smoothed & normalized) ----------
    float amp = u_amp;

    // Use separate angles from each noise field so directions vary locally
    float angle_f1 = (n1  - 0.5) * 6.2831853;   // -π..π
    float angle_f2 = (n2  - 0.5) * 6.2831853;
    float angle_s1 = (n1s - 0.5) * 6.2831853;
    float angle_s2 = (n2s - 0.5) * 6.2831853;

    // Fast layer direction: blend two independent angles
    vec2 v_fast = normalize(
        vec2(
            cos(angle_f1) + 0.7 * cos(angle_f2 + 1.37),
            sin(angle_f1) + 0.7 * sin(angle_f2 + 1.37)
        )
    );

    // Slow layer direction: different combination so it doesn't align
    vec2 v_slow = normalize(
        vec2(
            cos(angle_s1 + 0.83) + 0.6 * cos(angle_s2 - 2.41),
            sin(angle_s1 + 0.83) + 0.6 * sin(angle_s2 - 2.41)
        )
    );

    // Magnitude seeds, roughly in 0..1
    float mag_fast = (n1  * 0.5 + n2  * 0.5);
    float mag_slow = (n1s * 0.5 + n2s * 0.5);

    // Shape magnitudes to be smoother and avoid sharp sign flips
    mag_fast = mag_fast - 0.5;    // -0.5..0.5
    mag_slow = mag_slow - 0.5;
    mag_fast = mag_fast * abs(mag_fast);
    mag_slow = mag_slow * abs(mag_slow);

    // Smooth per-pixel orientation offset so blobs don't all align perfectly
    float local_phase = fbm(U * 0.7 + vec2(15.3, -9.1)); // 0..1
    float rot = (local_phase - 0.5) * 0.9;               // moderate ± rotation
    float c = cos(rot);
    float s = sin(rot);
    mat2 R = mat2(c, -s, s, c);

    vec2 v_fast_local = normalize(R * v_fast);
    vec2 v_slow_local = normalize(R * v_slow);

    // Combine into a displacement field (fast + slow, locally rotated)
    vec2 disp_unit =
        v_fast_local * mag_fast * 1.0 +
        v_slow_local * mag_slow * 0.6;

    // Normalize to avoid "pimples" when amp is high
    float len_d = length(disp_unit);
    if (len_d > 1.0) {
        disp_unit /= len_d;
    }

    // Final displacement – a bit stronger, still smooth
    vec2 disp = disp_unit * amp * 1.0 * motion_mask;

    float dx = disp.x;
    float dy = disp.y;

    vec2 warped_uv = uv + vec2(dx, dy);

    // Clamp to inside to avoid black borders
    warped_uv = clamp(warped_uv, vec2(0.0), vec2(1.0));

    vec3 col = texture(u_tex, warped_uv).rgb;

    // ---------- Simple vignette (soft) ----------
    float r2 = dot(p, p); // 0 center, 2 at corner
    float vig = 1.0 - 0.12 * r2;
    if(u_is_dark == 1){
        vig = 1.0 - 0.22 * r2;
    }
    col *= vig;

    // ---------- Breathing brightness (very subtle, more at edges) ----------
    float breath = 0.5 + 0.5 * sin(loopT);  // 1 breath per loop
    float edge_weight = 1.0 - smooth3(1.0 - r2);  // ~0 at center, ~1 towards corners
    float breath_strength = (u_is_dark == 1 ? 0.020 : 0.012);
    float brightness = 1.0 + breath_strength * (breath - 0.5) * edge_weight;
    col *= brightness;

    // ---------- Soft directional sweep (disabled for neutral motion) ----------
    // Keep the code for future use, but set strength to zero so it doesn't bias direction.
    float sweep = dot(normalize(vec2(0.7, 0.4)), p);
    float band = 0.5 + 0.5 * sin(loopT * 1.0 + sweep * 1.2);
    float sweep_strength  = 0.0;  // was (u_is_dark == 1 ? 0.012 : 0.007)
    float sweep_mask = 1.0 - 0.6 * safe;
    float light = 1.0 + sweep_strength * (band - 0.5) * sweep_mask;
    col *= light;

    // ---------- Chromatic micro-drift (weaker, looped) ----------
    float hue_phase = sin(2.0 * 3.14159265 * 2.0 * phase); // 2 cycles per loop
    float hue_amount = 0.006 * hue_phase;  // was 0.012, now more subtle

    float r = col.r;
    float g = col.g;
    float b = col.b;

    if(u_is_dark == 1){
        col.r = r * (1.0 - 0.3 * hue_amount);
        col.b = b * (1.0 + 0.5 * hue_amount);
    } else {
        col.g = g * (1.0 + 0.4 * hue_amount);
        col.b = b * (1.0 - 0.4 * hue_amount);
    }

    // --- Micro-dithering on dark theme to soften banding ---
    if(u_is_dark == 1){
        float dither = (hash(U * vec2(1973.0, 2843.0)) - 0.5) * (1.0 / 255.0);
        col += vec3(dither);
    }

    f_color = vec4(clamp(col, 0.0, 1.0), 1.0);
}
"""


def load_texture(ctx: moderngl.Context, path: str, size: tuple[int, int]) -> moderngl.Texture:
    w, h = size
    img = Image.open(path).convert("RGB")
    img = img.resize((w, h), Image.LANCZOS)
    data = img.tobytes()
    tex = ctx.texture((w, h), 3, data)
    tex.build_mipmaps()
    tex.repeat_x = False
    tex.repeat_y = False
    tex.filter = (moderngl.LINEAR_MIPMAP_LINEAR, moderngl.LINEAR)
    return tex


def render_gpu(cfg: RenderConfig) -> None:
    ctx = moderngl.create_standalone_context()
    prog = ctx.program(vertex_shader=VERT_SHADER, fragment_shader=FRAG_SHADER)

    # Fullscreen quad
    quad = np.array([
        -1.0, -1.0,  0.0, 0.0,
         1.0, -1.0,  1.0, 0.0,
        -1.0,  1.0,  0.0, 1.0,
         1.0,  1.0,  1.0, 1.0,
    ], dtype="f4")

    vbo = ctx.buffer(quad.tobytes())
    # Interleaved: vec2 position + vec2 uv = "2f 2f"
    vao = ctx.vertex_array(
        prog,
        [(vbo, "2f 2f", "in_pos", "in_uv")],
    )

    tex = load_texture(ctx, cfg.src_path, (cfg.width, cfg.height))
    tex.use(location=0)
    prog["u_tex"].value = 0
    # Use motion_level as a global multiplier on amp
    prog["u_amp"].value = cfg.amp * cfg.motion_level
    prog["u_is_dark"].value = 1 if cfg.is_dark else 0

    # Loop length and configurable safe UI zone
    prog["u_loop"].value = float(cfg.seconds)
    prog["u_safeRect"].value = cfg.safe_rect

    fbo = ctx.simple_framebuffer((cfg.width, cfg.height))
    fbo.use()

    total_frames = int(cfg.seconds * cfg.fps)

    # Diagnostics: keep track of motion & loop seam
    first_frame = None
    last_frame = None
    prev_frame = None
    acc_step_diff = 0.0
    step_count = 0
    
    # imageio's "quality" is 1..10, not a VP9 CRF.
    q = int(cfg.crf)
    if q < 1 or q > 10:
        q = 9

    writer = imageio.get_writer(
        cfg.out_path,
        mode="I",
        fps=cfg.fps,
        codec="libvpx-vp9",
        quality=q,
        macro_block_size=None,
    )

    print(f"[GPU] Rendering {cfg.out_path} ({cfg.width}x{cfg.height}, {total_frames} frames)…")

    for i in range(total_frames):
        t = i / float(cfg.fps)
        prog["u_time"].value = t

        ctx.clear(0.0, 0.0, 0.0, 1.0)
        vao.render(moderngl.TRIANGLE_STRIP)

        data = fbo.read(components=3, alignment=1)
        frame = np.frombuffer(data, dtype=np.uint8).reshape((cfg.height, cfg.width, 3))
        writer.append_data(frame)

        # --- Self-check accumulation ---
        if first_frame is None:
            first_frame = frame.copy()
        last_frame = frame.copy()
        if prev_frame is not None:
            acc_step_diff += float(
                np.mean(
                    np.abs(
                        prev_frame.astype(np.int16) - frame.astype(np.int16)
                    )
                )
            )
            step_count += 1
        prev_frame = frame

        if (i + 1) % max(1, cfg.fps * 2) == 0:
            print(f"  frame {i+1}/{total_frames}")

    writer.close()
    print(f"[GPU] Done → {cfg.out_path}")

    # Self-check: loop seam & motion diagnostics
    if first_frame is not None and last_frame is not None:
        seam_diff = float(
            np.mean(
                np.abs(
                    first_frame.astype(np.int16) - last_frame.astype(np.int16)
                )
            )
        )
        avg_step = float(acc_step_diff / step_count) if step_count > 0 else 0.0

        print(f"[CHECK] avg frame-to-frame diff: {avg_step:.3f} (0–255 scale)")
        print(f"[CHECK] start vs end frame diff: {seam_diff:.3f} (0–255 scale)")

        if seam_diff > max(1.5, avg_step * 1.5):
            print(
                "[CHECK] WARN: loop may not be perfectly seamless "
                "(start/end diff relatively high)."
            )
        else:
            print("[CHECK] Loop seam looks consistent with internal motion.")


def main():
    here = os.path.abspath(os.path.dirname(__file__))

    light_png = os.path.join(here, "light_theme.png")
    dark_png = os.path.join(here, "dark_theme.png")

    # DARK – calm but clearly alive
    dark_cfg = RenderConfig(
        src_path=dark_png,
        out_path=os.path.join(here, "background_dark_gpu.webm"),
        width=2048,
        height=1152,
        seconds=30.0,
        fps=30,
        amp=0.6,              # lower base, but...
        is_dark=True,
        crf=10,
        motion_level=0.5,      # ...full motion (was 0.2 → too little warp)
        safe_rect=(0.18, 0.20, 0.82, 0.80),
    )
    render_gpu(dark_cfg)

    # LIGHT – a bit softer
    light_cfg = RenderConfig(
        src_path=light_png,
        out_path=os.path.join(here, "background_light_gpu.webm"),
        width=2048,
        height=1152,
        seconds=30.0,
        fps=30,
        amp=0.3,
        is_dark=False,
        crf=10,
        motion_level=0.55,
        safe_rect=(0.20, 0.22, 0.80, 0.78),
    )
    render_gpu(light_cfg)


if __name__ == "__main__":
    main()
