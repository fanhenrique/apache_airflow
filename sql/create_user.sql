CREATE TABLE IF NOT EXISTS user_table (
  ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name VARCHAR(100),
  date date
);