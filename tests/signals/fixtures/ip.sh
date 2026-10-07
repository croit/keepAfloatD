#!/bin/sh
printf '%s\n' "$*" >> ip-commands
case "$CLEANUP_FIXTURE_MODE:$*" in
  discovery-failed:*) exit 2 ;;
  address-present:*' addr del '*) exit 2 ;;
  address-present:*' -o addr show to '*) printf 'address still present\n'; exit 0 ;;
  absent:*' addr del '*) exit 2 ;;
  absent:*' -o addr show to '*) exit 0 ;;
  marker-present:*' route del '*) exit 2 ;;
esac
case "$*" in
  '-N -j addr show')
    printf '%s\n' '[{"ifname":"cleanup0","addr_info":[{"family":"inet","local":"192.0.2.100","prefixlen":24},{"family":"inet6","local":"2001:db8::100","prefixlen":64},{"family":"inet","local":"192.0.2.101","prefixlen":24},{"family":"inet6","local":"2001:db8::101","prefixlen":64},{"family":"inet","local":"192.0.2.200","prefixlen":24},{"family":"inet","local":"192.0.2.201","prefixlen":24}]}]' ;;
  '-N -j -4 route show table all')
    printf '%s\n' '[{"type":"throw","dst":"192.0.2.101","protocol":245,"table":10245},{"type":"throw","dst":"192.0.2.201","protocol":246,"table":10246}]' ;;
  '-N -j -6 route show table all')
    printf '%s\n' '[{"type":"throw","dst":"2001:db8::101","protocol":245,"table":10245}]' ;;
  '-4 addr del 192.0.2.100/24 dev cleanup0' | \
  '-6 addr del 2001:db8::100/64 dev cleanup0' | \
  '-4 addr del 192.0.2.101/24 dev cleanup0' | \
  '-6 addr del 2001:db8::101/64 dev cleanup0' | \
  '-4 route del table 10245 throw 192.0.2.101/32 proto 245' | \
  '-6 route del table 10245 throw 2001:db8::101/128 proto 245') exit 0 ;;
  *) printf 'unexpected ip command: %s\n' "$*" >&2; exit 99 ;;
esac
