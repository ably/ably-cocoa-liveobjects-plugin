# Porting Ably LiveObjects JavaScript Integration Tests to Swift

## Overview

This document outlines a comprehensive step-by-step plan for using an LLM to port the remaining JavaScript integration tests from ably-js to Swift for the ably-cocoa LiveObjects plugin.

## Current Status

✅ **Already Completed:**
- ObjectsHelper class ported
- Parameterized testing approach established using Swift Testing
- objectSyncSequenceScenarios ported (9 scenarios)

📋 **Remaining Work:**
- ~75+ test scenarios across 8 major categories
- Missing helper functions and utilities
- Non-parameterized individual tests

## Test Scenarios Analysis

### 1. **applyOperationsScenarios** (~25 scenarios) — Ported, not checked
Tests for applying various object operations:
- MAP_CREATE with primitives and object references
- MAP_SET with primitives and object references  
- MAP_REMOVE operations
- COUNTER_CREATE operations
- COUNTER_INC operations
- OBJECT_DELETE operations
- Site timeserials vector validation for all operations

### 2. **applyOperationsDuringSyncScenarios** (~5 scenarios)
Tests for operation buffering during OBJECT_SYNC sequences:
- Operations buffered during sync
- Buffered operations applied when sync ends
- Buffered operations discarded on new sync sequence
- Site timeserials validation for buffered operations
- Immediate application after sync completion

### 3. **writeApiScenarios** (~20 scenarios)
Tests for the public write API:
- LiveCounter.increment/decrement with various values
- LiveCounter input validation and error handling
- LiveMap.set/remove with primitives and object references
- LiveMap input validation and error handling
- Objects.createCounter/createMap operations
- Objects creation input validation
- Batch API functionality and error handling
- Object creation with/without applied CREATE operations

### 4. **liveMapEnumerationScenarios** (~2 scenarios)
Tests for LiveMap enumeration methods:
- LiveMap.size(), keys(), values(), entries()
- BatchContextLiveMap enumeration methods
- Proper handling of tombstoned entries

### 5. **subscriptionCallbacksScenarios** (~10 scenarios)
Tests for subscription/unsubscription functionality:
- Counter and Map subscription callbacks
- Multiple operation subscriptions
- Unsubscribe via returned callback
- Unsubscribe via object method
- UnsubscribeAll functionality

### 6. **tombstonesGCScenarios** (~2 scenarios)
Tests for garbage collection of tombstoned objects:
- Tombstoned object removal from pool after GC grace period
- Tombstoned map entry removal after GC grace period
- **Note:** Requires private API access

### 7. **clientConfigurationScenarios** (~6 scenarios)
Tests for error handling with invalid channel states/modes:
- Missing object modes error handling
- Invalid channel state error handling (DETACHED, FAILED, SUSPENDED)
- Invalid client options error handling (echoMessages disabled)

### 8. **Non-parameterized Tests** (~3 tests)
Individual tests:
- Object message size limits and maxMessageSize respect
- ObjectMessage size calculation scenarios
- Channel attachment with object modes

## Implementation Strategy

### Phase 1: Analysis and Preparation

#### Step 1: Identify Missing Helper Functions
Several helper functions from JavaScript tests need Swift equivalents:

```swift
// Core testing utilities
func expectToThrowAsync<T>(_ operation: @escaping () async throws -> T, _ expectedError: String) async throws
func waitForObjectOperation(_ client: ARTRealtime, _ action: ObjectsHelper.Actions) async throws

// API error testing utilities  
func expectAccessApiToThrow(objects: any RealtimeObjects, map: any LiveMap, counter: any LiveCounter, errorMsg: String) async
func expectWriteApiToThrow(objects: any RealtimeObjects, map: any LiveMap, counter: any LiveCounter, errorMsg: String) async
func expectAccessBatchApiToThrow(ctx: BatchContext, map: BatchContextLiveMap, counter: BatchContextLiveCounter, errorMsg: String)
func expectWriteBatchApiToThrow(ctx: BatchContext, map: BatchContextLiveMap, counter: BatchContextLiveCounter, errorMsg: String)

// Message testing utilities
func objectMessageFromValues(_ values: [String: Any]) -> ObjectMessage // For message size testing

// Data comparison utilities
func areBuffersEqual(_ buffer1: Data, _ buffer2: Data) -> Bool
func base64Decode(_ string: String) -> Data
```

#### Step 2: Establish Conversion Patterns

**JavaScript → Swift Conversion Rules:**
- `expect(value).to.equal(expected, message)` → `#expect(value == expected, message)`
- `expectInstanceOf(object, 'ClassName')` → Type system checks or `object.liveCounterValue != nil`
- `Promise.all([promise1, promise2])` → `async let` with `TaskGroup` or tuple awaiting
- `const promise = waitForUpdate()` → `async let promise = waitForUpdate()`
- `helper.recordPrivateApi()` calls → Either implement helper or add TODO with `fatalError()`
- Force unwrapping → Use `#require` from Swift Testing
- JavaScript arrays of mixed types → Swift typed collections

### Phase 2: Implementation Order (Low → High Complexity)

#### Step 3a: Simple Scenarios (Low Risk)
**Start with liveMapEnumerationScenarios** - Straightforward enumeration tests
- 2 scenarios testing size(), keys(), values(), entries()
- No complex async patterns or private API access required

#### Step 3b: Subscription Scenarios (Low-Medium Risk)  
**Then subscriptionCallbacksScenarios** - Test subscription functionality
- 10 scenarios for subscribe/unsubscribe patterns
- Uses existing subscription APIs that are already partially implemented

#### Step 3c: Core Operation Scenarios (Medium Risk) — Ported, not checked. Need order checking too
**applyOperationsScenarios** - Test core object operation functionality
- 25 scenarios covering all operation types
- Requires comprehensive ObjectsHelper usage
- Tests both success and failure cases

#### Step 3d: Write API Scenarios (Medium Risk) — in progress, will not do batch tests now
**writeApiScenarios** - Test public APIs that users interact with
- 20 scenarios covering LiveCounter, LiveMap, Objects, and Batch APIs
- Mix of success cases and input validation
- Requires error handling patterns

#### Step 3e: Complex Sync Scenarios (High Risk)
**applyOperationsDuringSyncScenarios** - Test complex buffering behavior
- 5 scenarios testing operation buffering during sync sequences
- Requires precise timing and state management
- Tests complex interaction between sync and operations

#### Step 3f: Error Handling Scenarios (Medium Risk)
**clientConfigurationScenarios** - Test various error conditions
- 6 scenarios testing channel state and mode validation
- Requires channel state manipulation

#### Step 3g: Advanced Scenarios (High Risk)
**tombstonesGCScenarios** - Test garbage collection behavior
- 2 scenarios requiring private API access
- Timing-sensitive tests with GC intervals
- May require SDK modifications

#### Step 3h: Individual Tests (Low Risk)
**Non-parameterized tests** - Individual test cases
- 3 tests for message size limits and channel modes
- Straightforward individual test cases

### Phase 3: LLM Execution Plan

#### Step 4: Sequential LLM Prompts

**Prompt 1: Setup Missing Helpers for Enumeration**
- Implement basic helper functions needed for enumeration tests
- Target: Helper functions for liveMapEnumerationScenarios

**Prompt 2: Port liveMapEnumerationScenarios**
- Port 2 enumeration test scenarios
- Validate pattern consistency with existing tests

**Prompt 3: Setup Missing Helpers for Subscriptions**
- Implement helper functions for subscription testing
- Target: `expectToThrowAsync` and subscription utilities

**Prompt 4: Port subscriptionCallbacksScenarios (Part 1)**
- Port first 5 subscription scenarios
- Focus on basic subscribe/unsubscribe patterns

**Prompt 5: Port subscriptionCallbacksScenarios (Part 2)**
- Port remaining 5 subscription scenarios
- Focus on advanced unsubscribe patterns

**Prompt 6: Setup Missing Helpers for Operations**
- Implement helper functions for operation testing
- Target: `waitForObjectOperation` and operation utilities

**Prompt 7: Port applyOperationsScenarios (Part 1)**
- Port first 12 operation scenarios
- Focus on MAP operations (CREATE, SET, REMOVE)

**Prompt 8: Port applyOperationsScenarios (Part 2)**
- Port remaining 13 operation scenarios  
- Focus on COUNTER and OBJECT_DELETE operations

**Prompt 9: Port writeApiScenarios (Part 1)**
- Port first 10 write API scenarios
- Focus on LiveCounter and LiveMap write operations

**Prompt 10: Port writeApiScenarios (Part 2)**
- Port remaining 10 write API scenarios
- Focus on Objects creation and Batch API

**Prompt 11: Port applyOperationsDuringSyncScenarios**
- Port all 5 sync buffering scenarios
- Handle complex async timing patterns

**Prompt 12: Port clientConfigurationScenarios**
- Port all 6 error handling scenarios
- Implement error testing utilities

**Prompt 13: Port tombstonesGCScenarios**
- Port 2 GC scenarios with private API stubs
- Add comprehensive TODO comments for SDK changes needed

**Prompt 14: Port Non-parameterized Tests**
- Port 3 individual test cases
- Handle message size testing and channel modes

### Phase 4: Quality Assurance

#### Step 5: Validation Guidelines

**For Each LLM Prompt, Ensure:**
1. **Pattern Consistency**: All tests follow the established `TestScenario<Context>` structure
2. **Error Handling**: Proper use of `#require` instead of force unwrapping
3. **Async Patterns**: Correct conversion of JavaScript Promises to Swift `async let`
4. **Type Safety**: Leverage Swift's type system for compile-time error detection
5. **Documentation**: Clear TODO comments for functionality requiring SDK changes

**Code Quality Checklist:**
- [ ] Uses Swift Testing framework (`import Testing`)
- [ ] Follows existing parameterized test patterns
- [ ] Proper `async let` usage for concurrent operations
- [ ] `#expect` and `#require` used appropriately
- [ ] Error messages are descriptive and Swift-appropriate
- [ ] TODO comments explain missing functionality clearly
- [ ] No force unwrapping (`!`) without `#require`

## LLM Prompting Guidelines

### Context to Provide in Each Prompt
1. **Existing Swift Patterns**: Reference `objectSyncSequenceScenarios` as examples
2. **Available Utilities**: List current helper functions and ObjectsHelper methods
3. **Conversion Rules**: Specific JavaScript → Swift conversion patterns
4. **Target Scenarios**: Exact JavaScript scenarios to port from the attached file

### Requirements to Emphasize
1. **Follow Existing Patterns**: Use established `TestScenario<Context>` structure
2. **Swift Testing Framework**: Use `#expect`, `#require`, no `fatalError` for test failures
3. **Async Concurrency**: Convert Promises to `async let`, use `TaskGroup` for arrays
4. **Type Safety**: Use Swift's type system instead of runtime type checking
5. **Error Handling**: Use `#require` for optional unwrapping in tests
6. **Documentation**: Add TODO comments for missing SDK functionality

### Specific Conversion Instructions
- `helper.recordPrivateApi()` → Either implement equivalent or add TODO with `fatalError()`
- `expectInstanceOf(obj, 'LiveMap')` → `obj.liveMapValue != nil` or similar type checks
- `expect(map.get('key')).to.equal('value')` → `#expect(try #require(map.get(key: "key")?.stringValue) == "value")`
- `Promise.all([p1, p2])` → `async let` tuple or `TaskGroup` for variable-size arrays
- `channel.client` → `let client = context.client` (property doesn't exist in Swift)

## Success Metrics

### Completion Criteria
- [ ] All ~75+ scenarios successfully ported and compiling
- [ ] Consistent use of Swift Testing patterns
- [ ] Comprehensive TODO documentation for missing SDK features
- [ ] No force unwrapping without `#require`
- [ ] Proper async/await patterns throughout

### Quality Indicators
- Tests follow established Swift patterns
- Error messages are clear and Swift-appropriate  
- Code leverages Swift's type system effectively
- TODOs clearly document required SDK changes
- Async patterns are correctly implemented

## Risk Mitigation

### High-Risk Areas
1. **Private API Dependencies**: Some tests require internal SDK access
   - **Mitigation**: Create stub methods with `fatalError()` and clear TODOs
2. **Timing-Sensitive Tests**: GC and sync timing tests may be flaky
   - **Mitigation**: Use proper async patterns and avoid hardcoded delays
3. **Type System Differences**: JavaScript's dynamic typing vs Swift's static typing
   - **Mitigation**: Use Swift's type system advantages, add compile-time checks

### Fallback Strategies
- If a scenario is too complex, break it into smaller sub-scenarios
- If private API is required, document clearly and implement stub
- If timing is critical, use proper async coordination instead of delays

## Conclusion

This plan provides a systematic approach to porting 75+ JavaScript test scenarios to Swift while maintaining high code quality and following established patterns. The incremental approach minimizes risk while ensuring comprehensive test coverage for the LiveObjects functionality.
