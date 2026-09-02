# SLIT Summary — metro subscriber (ISS-3650818)

## Source
- **Ticket**: ISS-3650818 — UPI Axis TO - Unreconciled Transaction
- **Root Cause**: ForceAuthRRNMismatch PRS event not configured for bank_same_day_settlement file type
- **Service**: metro (pub-sub messaging platform)

## Scenarios

| ID | Title | Type | Priority |
|----|-------|------|----------|
| SCN-005 | PRS retrigger generates both rrn_mismatch and force_auth requests | SLIT | P1 |
| SCN-006 | Subscriber consumes recon records with missing PRS event config | SLIT | P1 |
| SCN-007 | Subscription health monitoring when downstream PRS processing fails | SLIT | P1 |
| SCN-008 | Recon status query for unreconciled records | SLIT | P2 |
| SCN-009 | Subscription backoff and retry interval calculation | Unit | P2 |

## Test Coverage

### SLIT Tests (4)
- **PRS Retrigger**: Verifies both rrn_mismatch and force_auth requests are generated
- **Subscriber with Missing Config**: Verifies subscriber consumes messages even when downstream config is missing
- **Subscription Health**: Verifies health monitoring doesn't conflate downstream failures with subscriber failures
- **Recon Status Query**: Verifies status query returns correct unreconciled status

### Unit Tests (1)
- **Backoff Calculation**: Verifies exponential and fixed window backoff calculations including edge cases

## Build Status
- Package: `slit/internal/subscriber`
- Dependencies: gomock, testify/assert, testify/require
- Build tags: none (standard Go tests)

## Notes
- The metro service is a pub-sub messaging platform that delivers recon records to consumers
- The root cause (missing PRS config) is a configuration issue, not a code defect
- Tests focus on metro's role: message delivery, subscription health, and retry behavior
- The backoff test extends existing backoff_test.go coverage with edge cases
