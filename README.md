# Mixpanel Phish
a Mixpanel project to track the greatest rock band of our time (and their fans)


actual docs todo... but we're trying to model something like this:

```mermaid
flowchart TD
    A["Phan"] -->|attends| B(Show)
    A["Phan"] -->|witness| C(Performance)
    A["Phan"] -->|reviews| B(Show)
    D["Song"] ---|played at| B(Show)
    B["Show"] ---|is a| C(Performance)
    A["Phan"] -->|visits| E(Venue)
    B["Show"] --> |happened| E(Venue)
    A["Phan"] --> |enjoys| D(Song)
```

in mixpanel's ERD: https://docs.mixpanel.com/docs/data-structure/concepts