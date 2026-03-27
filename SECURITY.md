# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please send a description of the vulnerability to [security@databricks.com](mailto:security@databricks.com). Include:

- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide a more detailed response within 5 business days.

## Scope

This repository contains workshop materials and synthetic data generators. It does not include production services or handle real customer data. Security concerns most likely relate to:

- Overly permissive SQL in notebook cells
- Credential handling in setup/teardown scripts
- Dependencies with known vulnerabilities
