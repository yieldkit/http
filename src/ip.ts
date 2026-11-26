/**
 * Parsed IPv4 or IPv6 address held as an integer, so ranges compare with shifts.
 */
export type ParsedIpAddress = {
  /**
   * IP address family.
   */
  readonly version: 4 | 6;

  /**
   * Numeric address value.
   */
  readonly value: bigint;

  /**
   * Number of address bits for the family.
   */
  readonly bits: 32 | 128;
};

/**
 * Address range written as a network address and prefix length.
 */
type AddressRange = readonly [network: string, prefixLength: number];

/**
 * Number of dotted-decimal octets in an IPv4 address.
 */
const IPV4_OCTET_COUNT = 4;

/**
 * Largest value a single IPv4 octet can hold.
 */
const MAX_IPV4_OCTET = 255;

/**
 * Number of 16-bit groups (hextets) in a fully written IPv6 address.
 */
const IPV6_HEXTET_COUNT = 8;

/**
 * Mask selecting the low 32 bits of an address, where an embedded IPv4 address sits.
 */
const IPV4_MASK = 0xff_ff_ff_ffn;

/**
 * Bit offset of the IPv4 address embedded in a 6to4 (`2002::/16`) address.
 */
const SIX_TO_FOUR_IPV4_SHIFT = 80n;

/**
 * One octet of a dotted IPv4 address, with no leading zero in a multi-digit octet.
 */
const IPV4_OCTET_PATTERN = /^(?:0|[1-9]\d{0,2})$/u;

/**
 * One hextet of an IPv6 address.
 */
const IPV6_HEXTET_PATTERN = /^[0-9a-f]{1,4}$/u;

/**
 * An address wrapped in URL brackets, such as `[::1]`.
 */
const BRACKETED_PATTERN = /^\[(.*)\]$/u;

/**
 * IPv4 ranges that are unspecified, private, shared, loopback, link-local, protocol assignments,
 * documentation, 6to4 relay, benchmarking, multicast, reserved or broadcast.
 */
const NON_PUBLIC_IPV4_RANGES: ReadonlyArray<AddressRange> = [
  ['0.0.0.0', 8],
  ['10.0.0.0', 8],
  ['100.64.0.0', 10],
  ['127.0.0.0', 8],
  ['169.254.0.0', 16],
  ['172.16.0.0', 12],
  ['192.0.0.0', 24],
  ['192.0.2.0', 24],
  ['192.88.99.0', 24],
  ['192.168.0.0', 16],
  ['198.18.0.0', 15],
  ['198.51.100.0', 24],
  ['203.0.113.0', 24],
  ['224.0.0.0', 4],
  ['240.0.0.0', 4],
  ['255.255.255.255', 32],
];

/**
 * IPv6 ranges that carry an IPv4 address in their low 32 bits and are judged by it: IPv4-mapped and
 * the well-known NAT64 prefix.
 */
const IPV4_EMBEDDING_IPV6_RANGES: ReadonlyArray<AddressRange> = [
  ['::ffff:0:0', 96],
  ['64:ff9b::', 96],
];

/**
 * The 6to4 range, which carries an IPv4 address in bits 16 to 47.
 */
const SIX_TO_FOUR_RANGE: AddressRange = ['2002::', 16];

/**
 * IPv6 global unicast space. Everything outside it, including `::`, `::1`, `fc00::/7`, `fe80::/10`,
 * `fec0::/10`, `ff00::/8`, `100::/64` and the local-use NAT64 prefix, is non-public.
 */
const GLOBAL_UNICAST_IPV6_RANGE: AddressRange = ['2000::', 3];

/**
 * Ranges carved out of IPv6 global unicast space: IETF protocol assignments (including Teredo
 * `2001::/32` and benchmarking) and the two documentation prefixes.
 */
const NON_PUBLIC_GLOBAL_IPV6_RANGES: ReadonlyArray<AddressRange> = [
  ['2001::', 23],
  ['2001:db8::', 32],
  ['3fff::', 20],
];

/**
 * Parses an IPv4 or IPv6 literal, with or without URL brackets. Returns null for anything else,
 * including IPv6 addresses with a zone index (`fe80::1%en0`).
 */
export const parseIpAddress = (value: string): ParsedIpAddress | null => {
  const unbracketed = value.replace(BRACKETED_PATTERN, '$1');
  return unbracketed.includes(':') ? parseIpv6Address(unbracketed) : parseIpv4Address(unbracketed);
};

/**
 * Returns `true` when `address` is a publicly routable IPv4 or IPv6 literal. Private, loopback,
 * link-local, shared, documentation, benchmarking, multicast and reserved addresses are not public,
 * and neither is anything that does not parse as an IP address. IPv4-mapped (`::ffff:0:0/96`),
 * NAT64 (`64:ff9b::/96`) and 6to4 (`2002::/16`) addresses are judged by the IPv4 address they
 * embed. Brackets are accepted, so `[::1]` is parsed as `::1`.
 */
export const isPublicIpAddress = (address: string): boolean => {
  const parsed = parseIpAddress(address);
  return parsed !== null && isPublicParsedAddress(parsed);
};

/**
 * Applies the public-address rules to an already parsed address.
 */
const isPublicParsedAddress = (address: ParsedIpAddress): boolean => {
  if (address.version === 4) {
    return !NON_PUBLIC_IPV4_RANGES.some((range) => isInRange(address, range));
  }

  const embedded = embeddedIpv4(address);
  if (embedded !== null) {
    return isPublicParsedAddress(embedded);
  }

  return (
    isInRange(address, GLOBAL_UNICAST_IPV6_RANGE) &&
    !NON_PUBLIC_GLOBAL_IPV6_RANGES.some((range) => isInRange(address, range))
  );
};

/**
 * Returns the IPv4 address carried by an IPv4-mapped, NAT64 or 6to4 address, or null.
 */
const embeddedIpv4 = (address: ParsedIpAddress): ParsedIpAddress | null => {
  if (IPV4_EMBEDDING_IPV6_RANGES.some((range) => isInRange(address, range))) {
    return { version: 4, value: address.value & IPV4_MASK, bits: 32 };
  }

  if (isInRange(address, SIX_TO_FOUR_RANGE)) {
    return { version: 4, value: (address.value >> SIX_TO_FOUR_IPV4_SHIFT) & IPV4_MASK, bits: 32 };
  }

  return null;
};

/**
 * Returns `true` when `address` falls inside a network range of the same family.
 */
const isInRange = (address: ParsedIpAddress, [network, prefixLength]: AddressRange): boolean => {
  const parsedNetwork = parseIpAddress(network);
  if (parsedNetwork === null || parsedNetwork.version !== address.version) {
    return false;
  }

  const shift = BigInt(address.bits - prefixLength);
  return address.value >> shift === parsedNetwork.value >> shift;
};

/**
 * Parses a dotted-decimal IPv4 address with exactly four decimal octets.
 */
const parseIpv4Address = (value: string): ParsedIpAddress | null => {
  const octets = value.split('.');
  if (octets.length !== IPV4_OCTET_COUNT) {
    return null;
  }

  let numeric = 0n;
  for (const part of octets) {
    const octet = IPV4_OCTET_PATTERN.test(part) ? Number.parseInt(part, 10) : Number.NaN;
    if (Number.isNaN(octet) || octet > MAX_IPV4_OCTET) {
      return null;
    }

    numeric = (numeric << 8n) + BigInt(octet);
  }

  return { version: 4, value: numeric, bits: 32 };
};

/**
 * Parses an IPv6 address, including compressed and IPv4-tail forms.
 */
const parseIpv6Address = (value: string): ParsedIpAddress | null => {
  if (value.includes('%')) {
    return null;
  }

  const compressedParts = value.toLowerCase().split('::');
  if (compressedParts.length > 2) {
    return null;
  }

  const hasCompression = compressedParts.length === 2;
  const left = parseIpv6HextetList(compressedParts[0] ?? '', !hasCompression);
  const right = parseIpv6HextetList(compressedParts[1] ?? '', true);
  if (left === null || right === null) {
    return null;
  }

  const missingHextets = IPV6_HEXTET_COUNT - left.length - right.length;
  const validLength = hasCompression ? missingHextets >= 1 : missingHextets === 0;
  if (!validLength) {
    return null;
  }

  let numeric = 0n;
  for (const hextet of [...left, ...Array.from({ length: missingHextets }, () => 0), ...right]) {
    numeric = (numeric << 16n) + BigInt(hextet);
  }

  return { version: 6, value: numeric, bits: 128 };
};

/**
 * Parses one side of an IPv6 address into hextets. An IPv4 tail is allowed only as the last group
 * of the address.
 */
const parseIpv6HextetList = (value: string, allowIpv4Tail: boolean): Array<number> | null => {
  if (value.length === 0) {
    return [];
  }

  const parts = value.split(':');
  const hextets: Array<number> = [];
  for (const [index, part] of parts.entries()) {
    if (part.includes('.')) {
      const ipv4 = allowIpv4Tail && index === parts.length - 1 ? parseIpv4Address(part) : null;
      if (ipv4 === null) {
        return null;
      }

      hextets.push(Number((ipv4.value >> 16n) & 0xff_ffn), Number(ipv4.value & 0xff_ffn));
      continue;
    }

    if (!IPV6_HEXTET_PATTERN.test(part)) {
      return null;
    }

    hextets.push(Number.parseInt(part, 16));
  }

  return hextets;
};
