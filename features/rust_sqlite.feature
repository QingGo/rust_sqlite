# -- FILE: features/example.feature
Feature: Check if rust_sqlite run porperly

  Scenario: inserts and retrieves a row
    Given open rust_sqlite binary
    When execute some sql commands
      """
      insert 1 user1 person1@example.com
      select
      .exit
      """
    Then get expected stdout
      """
      db > Executed
      db > (1, user1, person1@example.com)
      Executed
      db >
      """

  # Scenario: prints error message when table is full
  #   Given open rust_sqlite binary
  #   When insert many rows
  #   Then get expected error

  Scenario: allows inserting strings that are the maximum length
    Given open rust_sqlite binary
    When inserting strings that are the maximum length
    Then get expected maximum string stdout

  Scenario: prints error message if strings are too long
    Given open rust_sqlite binary
    When inserting strings that are longer than the maximum length
    Then get string is too long error

  Scenario: keeps data after closing connection
    Given open rust_sqlite binary
    When execute some sql commands
      """
      insert 1 user1 person1@example.com
      .exit
      """
    Then get expected stdout
      """
      db > Executed
      db >
      """
    When reopen rust_sqlite binary
    And execute some sql commands
      """
      select
      .exit
      """
    Then get expected stdout
      """
      db > (1, user1, person1@example.com)
      Executed
      db >
      """
    When reopen rust_sqlite binary
    And execute some sql commands
      """
      insert 2 user2 person2@example.com
      .exit
      """
    And reopen rust_sqlite binary
    And execute some sql commands
      """
      select
      .exit
      """
    Then get expected stdout
      """
      db > (1, user1, person1@example.com)
      (2, user2, person2@example.com)
      Executed
      db >
      """

  Scenario: prints constants
    Given open rust_sqlite binary
    When execute some sql commands
      """
      .constants
      .exit
      """
    Then get expected stdout
      """
      db > Constants:
      ROW_SIZE: 291
      COMMON_NODE_HEADER_SIZE: 6
      LEAF_NODE_HEADER_SIZE: 10
      LEAF_NODE_CELL_SIZE: 295
      LEAF_NODE_SPACE_FOR_CELLS: 4086
      LEAF_NODE_MAX_CELLS: 13
      db >
      """

  Scenario: allows printing out the structure of a one-node btree
    Given open rust_sqlite binary
    When execute some sql commands
      """
      insert 3 user2 person2@example.com
      insert 1 user2 person2@example.com
      insert 2 user2 person2@example.com
      .btree
      .exit
      """
    Then get expected stdout
      """
      db > Executed
      db > Executed
      db > Executed
      db > Tree:
      leaf (size 3)
      - 0 : 3
      - 1 : 1
      - 2 : 2
      db >
      """