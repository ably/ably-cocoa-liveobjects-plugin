# Task: Finish implementing serialization and deserialization of WireObjectMessage

## Definitions

When I refer to the _Specification Document_, I mean the contents of the file `textile/features.textile` in the repository https://github.com/ably/specification at commit `9f719f9`.

## Background

The Specification Document contains the definition of a type called `ObjectMessage`. The Specification Document describes this type in two ways:

1. as text inside the document
2. via a bespoke Interface Definition Language (IDL) at the end of the document

An `ObjectMessage` is serializable to and from JSON.

An `ObjectMessage` has various properties, such as `operation`, `object`, etc. Some of these properties are of plain JSON types, and some are of othe types which are also defined in the Specification Document, such as `WireObjectOperation`.

The file `WireObjectMessage.swift` contains a type called `WireObjectMessage`. This is a type that represents an `ObjectMessage`. I have added example implementations of the `JSONObjectCodable` protocol showing how to serialize it to and from JSON. These examples use our codebase's internal `JSONValue` type as well as helper methods that we have used for extracting `JSONValue`s from a dictionary.

Your task is to:

- Finish implementing the `WireObjectMessage` type, adding any new types as required and implementing JSON serialization and deserialization for the placeholder types which have not yet been implemented.

Things to _not_ do:

- Do not implement `WireObjectMessage`'s `id`, `connectionId`, or `timestamp` properties.
- Do not implement `ObjectData`'s `value` property.

Guidance:

- If in doubt about the approach to take, look at the existing example of `WireObjectMessage`.
- When implementing an enum, make use of the existing `WireEnum` type to enable handling of currently-unknown enum values.
- Add comments next to each of the new properties, giving the identifier of the relevant specification point, e.g. OM2b. Do not add any comments beyond the identifier of the specification points.
- Add all of your new types to the existing `WireObjectMessage.swift` file.
- Make sure all new types, except for enums (which should use the same naming as in the Specification Document) have their names prefixed with `Wire`.

Feedback based on your previous attempts:

- Do _not_ edit any of the existing implementation of `WireObjectMessage`.
- Give all new properties and types an access level of `internal`.
- Note that the Specification Document says that all enums are integer-valued.
