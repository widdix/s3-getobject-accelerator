const globals = require('globals');
const js = require('@eslint/js');
const { FlatCompat } = require('@eslint/eslintrc');

const compat = new FlatCompat({
  baseDirectory: __dirname,
  recommendedConfig: js.configs.recommended,
  allConfig: js.configs.all
});

module.exports = [...compat.extends('eslint:recommended'), {
  languageOptions: {
    globals: {
      ...globals.commonjs,
      ...globals.node,
      ...globals.mocha,
    },

    ecmaVersion: 2021,
    sourceType: 'commonjs',
  },

  rules: {
    indent: ['error', 2],
    'linebreak-style': ['error', 'unix'],
    quotes: ['error', 'single'],
    semi: ['error', 'always'],
  },
}];