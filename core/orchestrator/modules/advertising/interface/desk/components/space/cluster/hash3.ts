function fnv1a32(str: string, seed = 0x811c9dc5): number {
  let h = seed >>> 0;
  for (let i = 0; i < str.length; i += 1) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

function u32To01(x: number): number {
  return (x >>> 0) / 0xffffffff;
}

export function textToVec3(parts: string[]): [number, number, number] {
  const s = parts.map((x) => String(x ?? '').trim().toLowerCase()).filter(Boolean).join(' | ');
  const h1 = fnv1a32(s, 0x811c9dc5);
  const h2 = fnv1a32(s, 0x1234567);
  const h3 = fnv1a32(s, 0xdeadbeef);

  // 0..1
  return [u32To01(h1), u32To01(h2), u32To01(h3)];
}
