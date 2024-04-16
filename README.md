# Skylark

Skylark is desktop application intended for work with text data.
Can be helpful for Price Monitoring Departments and persons who work with products names.
You can easily work with .xlsx or .csv files and save a lot of time.

- Extract features from products names and convert them to regex
- Compare products by their names

Skylark consists of 3 main modules:

- SemantiX. This module extracts text features from products names and convert to regex.
  Extracted regex can be used for product identification among other products names.
- Feature Flow. This module intended for compare two products by them names.
  Module extracts text features from products names and compares them by some rules.
  Module looks like SemantiX, but has more options. Prediction is 0 or 1.
- Simfyzer. This module intended for compare two products by them names.
  Comparison is implemented via combination of Jakkar and Levenshtein distance.
  Approach to Comparison might be similar to Microsoft Fuzzy Lookup.
  Prediction is 0..1.

# Some features

- You can use your custom rules by configuring .json file in config dir or via GUI.
- You can add your custom measures by configuring .json file in config dir or via GUI.
- You don't really need to edit main config. You can create and use your custom configs.
- Main config and measures tested with custom and generic func tests (you can run 'em with pytest).

# Some problems

- v1 release don't have a lot of input validators. For example, if you type wrong column name that doesn't exist, app will carsh. Same with config - it doesn't have validation.
