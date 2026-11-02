# mholt/caddy-ratelimit is experimental and NOT in the stock caddy:2 image —
# it must be compiled in with xcaddy. If the Caddyfile adapter ever errors
# "rate_limit is not an ordered directive", add a global options block:
#   { order rate_limit before reverse_proxy }
FROM caddy:2-builder AS builder
RUN xcaddy build --with github.com/mholt/caddy-ratelimit

FROM caddy:2
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
