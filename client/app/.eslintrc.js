module.exports = {
  root: true,
  extends: [
    '@react-native',
    'prettier',
  ],
  overrides: [
    {
      files: ['**/*.ts', '**/*.tsx'],
      extends: [
        'plugin:@typescript-eslint/eslint-recommended',
        'plugin:@typescript-eslint/recommended',
        'plugin:@typescript-eslint/recommended-requiring-type-checking',
      ],
      parser: "@typescript-eslint/parser",
      parserOptions: {
          project: "./tsconfig.json",
          ecmaVersion: "latest",
          sourceType: "module",
      },
    },
  ],
};
