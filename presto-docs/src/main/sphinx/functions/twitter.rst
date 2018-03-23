==================
Twitter Functions
==================

These functions provide some convenience functionality commonly used at Twitter.

String Functions
-----------------

.. function:: split_every(string) -> array

    Splits ``string`` on every character and returns an array.

.. function:: split_every(string, length) -> array

    Splits ``string`` on every ``length`` characters and returns an array.
    length must be a positive number.

.. function:: split_every(string, length, limit) -> array

    Splits ``string`` on every ``length`` characters and returns an array of size at most ``limit``.
    The last element in the array always contains everything left in the string. ``limit`` must be a positive number.

Twitter IDs(Snowflake) Functions
---------------------------------

The utility functions for `Twitter IDs(Snowflake) <https://developer.twitter.com/en/docs/basics/twitter-ids>`_.

.. function:: is_snowflake(id) -> boolean

    Return if a bigint is a snowflake ID (true/false).

.. function:: first_snowflake_for(timestamp) -> bigint

    Return the first snowflake ID given a timestamp.

.. function:: timestamp_from_snowflake(id) -> timestamp

    Return the timestamp given a snowflake ID.

.. function:: cluster_id_from_snowflake(id) -> bigint

    Return the cluster ID given a snowflake ID.

.. function:: instance_id_from_snowflake(id) -> bigint

    Return the instance ID given a snowflake ID.

.. function:: sequence_num_from_snowflake(id) -> bigint

    Return the sequence number given a snowflake ID.
