# Security Policy

## Supported Versions

We release patches for security vulnerabilities. The project ships
date-versioned nightly releases; the latest nightly is the supported line:

| Version                     | Supported          |
| --------------------------- | ------------------ |
| latest nightly (`2026.x`)   | :white_check_mark: |
| `0.x` (legacy SemVer)       | :x:                |

## Reporting a Vulnerability

We take the security of Bamboo seriously. If you have discovered a security vulnerability, please report it to us.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to: **mugeng.du@gmail.com**

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

### What to Include

Please include the following information in your report:

- **Type of issue** (e.g., buffer overflow, SQL injection, cross-site scripting, etc.)
- **Full paths of source file(s)** related to the manifestation of the issue
- **The location of the affected source code** (tag/branch/commit or direct URL)
- **Any special configuration** required to reproduce the issue
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact of the issue**, including how an attacker might exploit it

### What to Expect

- **Acknowledgment**: We'll acknowledge receipt of your report within 48 hours
- **Assessment**: We'll assess the vulnerability and determine its severity
- **Fix**: We'll work on a fix and prepare a security release
- **Disclosure**: We'll coordinate disclosure with you

### Disclosure Policy

- We will **not** disclose the vulnerability until a fix is ready
- We will **credit you** in the security advisory (unless you prefer to remain anonymous)
- We request that you **do not disclose** the vulnerability publicly until we have released a fix

## Security Best Practices

When using Bamboo in production:

1. **Keep updated**: Always use the latest stable version
2. **Secure your API keys**: Never commit API keys to version control
3. **Use environment variables**: Store sensitive configuration in environment variables
4. **Enable rate limiting**: Use the built-in rate limiting features
5. **Configure CORS**: Properly configure CORS for your use case
6. **Regular audits**: Regularly audit your dependencies with `cargo audit`

## Security Features

Bamboo includes several built-in security features:

- ✅ **Rate Limiting**: Built-in protection against DoS attacks
- ✅ **CORS Configuration**: Configurable Cross-Origin Resource Sharing
- ✅ **Input Validation**: Comprehensive input validation and sanitization
- ✅ **Secure Headers**: Security headers for HTTP responses
- ✅ **Encrypted Storage**: Encrypted storage for sensitive data
- ✅ **API Key Protection**: Secure handling of LLM provider API keys

## Known Security Considerations

### API Keys

- API keys for LLM providers are stored in configuration files
- We recommend using environment variables for production deployments
- Never commit API keys to version control

### Network Security

- By default, Bamboo binds to `127.0.0.1` (localhost only)
- For production, configure appropriate firewall rules
- Use HTTPS in production environments

### Dependencies

We regularly audit our dependencies for known vulnerabilities:

```bash
# Run security audit
cargo audit
```

## Security Updates

Security updates will be released as patch versions and announced via:

- GitHub Security Advisories
- Release notes on GitHub
- crates.io updates

## Contact

For any security-related questions or concerns, contact:

- **Email**: mugeng.du@gmail.com
- **GitHub**: https://github.com/bigduu/Bamboo-agent/security

---

**Last Updated**: 2026-02-23
