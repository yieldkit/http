import { describe, expect, it } from 'vite-plus/test';

import { isPublicIpAddress, parseIpAddress } from './ip.ts';

describe('isPublicIpAddress', () => {
  it('rejects every non-public IPv4 range', () => {
    const addresses = [
      '0.0.0.0',
      '10.1.2.3',
      '100.64.0.1',
      '127.0.0.1',
      '169.254.169.254',
      '172.16.0.1',
      '172.31.255.255',
      '192.0.0.8',
      '192.0.2.1',
      '192.88.99.1',
      '192.168.1.1',
      '198.18.0.1',
      '198.19.255.255',
      '198.51.100.1',
      '203.0.113.1',
      '224.0.0.1',
      '240.0.0.1',
      '255.255.255.255',
    ];
    for (const address of addresses) {
      expect({ address, public: isPublicIpAddress(address) }).toEqual({ address, public: false });
    }
  });

  it('accepts public IPv4 addresses next to the private ranges', () => {
    for (const address of ['8.8.8.8', '1.1.1.1', '172.32.0.1', '100.128.0.1', '198.20.0.1']) {
      expect({ address, public: isPublicIpAddress(address) }).toEqual({ address, public: true });
    }
  });

  it.each(['012.0.0.1', '008.008.008.008'])(
    'rejects IPv4 octets with leading zeros in %s',
    (address) => {
      expect(isPublicIpAddress(address)).toBe(false);
    },
  );

  it('rejects every non-public IPv6 range', () => {
    const addresses = [
      '::',
      '::1',
      '[::1]',
      'fc00::1',
      'fd12:3456::1',
      'fe80::1',
      'fec0::1',
      'ff02::1',
      '2001:db8::1',
      '100::1',
      '2001::1',
      '3fff::1',
      '::7f00:1',
      '64:ff9b:1::1',
    ];
    for (const address of addresses) {
      expect({ address, public: isPublicIpAddress(address) }).toEqual({ address, public: false });
    }
  });

  it('judges IPv4-mapped, NAT64 and 6to4 addresses by their embedded IPv4 address', () => {
    expect(isPublicIpAddress('::ffff:127.0.0.1')).toBe(false);
    expect(isPublicIpAddress('::ffff:7f00:1')).toBe(false);
    expect(isPublicIpAddress('64:ff9b::7f00:1')).toBe(false);
    expect(isPublicIpAddress('64:ff9b::a9fe:a9fe')).toBe(false);
    expect(isPublicIpAddress('2002:7f00:1::1')).toBe(false);
    expect(isPublicIpAddress('2002:c0a8:101::1')).toBe(false);
    expect(isPublicIpAddress('::ffff:8.8.8.8')).toBe(true);
    expect(isPublicIpAddress('64:ff9b::808:808')).toBe(true);
    expect(isPublicIpAddress('2002:808:808::1')).toBe(true);
  });

  it('accepts global unicast IPv6 addresses', () => {
    expect(isPublicIpAddress('2606:4700:4700::1111')).toBe(true);
    expect(isPublicIpAddress('[2a00:1450:4001::200e]')).toBe(true);
  });

  it('treats anything that is not an IP literal as non-public', () => {
    for (const value of ['example.com', '', '1.2.3', '1.2.3.256', 'fe80::1%en0', '1::2::3']) {
      expect({ value, public: isPublicIpAddress(value) }).toEqual({ value, public: false });
    }
  });
});

describe('parseIpAddress', () => {
  it('parses compressed and IPv4-tail IPv6 forms to the same value', () => {
    expect(parseIpAddress('::ffff:127.0.0.1')).toEqual(parseIpAddress('::ffff:7f00:1'));
    expect(parseIpAddress('0:0:0:0:0:0:0:1')).toEqual(parseIpAddress('::1'));
  });

  it('rejects malformed addresses', () => {
    expect(parseIpAddress('1:2:3:4:5:6:7:8:9')).toBeNull();
    expect(parseIpAddress('1.2.3.4.5')).toBeNull();
    expect(parseIpAddress('01234::1')).toBeNull();
  });
});
