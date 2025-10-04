# CORS Configuration Guide

## Overview

Cross-Origin Resource Sharing (CORS) is implemented in this FastAPI application to control which origins can access the API endpoints. This guide explains how to configure CORS settings for different environments.

## Current Implementation

The CORS middleware is configured in `app/main.py` using FastAPI's `CORSMiddleware` with comprehensive settings defined in `app/core/config.py`.

### Configuration Options

| Environment Variable | Default Value | Description |
|---------------------|---------------|-------------|
| `ALLOWED_ORIGINS` | `["*"]` | Comma-separated list of allowed origins or "*" for all |
| `CORS_ALLOW_CREDENTIALS` | `true` | Whether to allow credentials in CORS requests |
| `CORS_ALLOW_METHODS` | `["GET", "POST", "PUT", "DELETE", "OPTIONS"]` | Allowed HTTP methods |
| `CORS_ALLOW_HEADERS` | `["*"]` | Allowed request headers |
| `CORS_EXPOSE_HEADERS` | `[]` | Headers exposed to the client |
| `CORS_MAX_AGE` | `600` | Cache time for preflight requests (seconds) |
| `CORS_ALLOW_ORIGIN_REGEX` | `None` | Regex pattern for allowed origins |

## Environment-Specific Configurations

### Development Environment

For local development with frontend running on different ports:

```bash
# Allow specific development origins
ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000,http://localhost:8080

# Or allow all origins (less secure)
ALLOWED_ORIGINS=*

# Enable credentials for authentication
CORS_ALLOW_CREDENTIALS=true

# Allow all common methods
CORS_ALLOW_METHODS=GET,POST,PUT,DELETE,OPTIONS

# Allow all headers for flexibility
CORS_ALLOW_HEADERS=*
```

### Production Environment

For production, be more restrictive:

```bash
# Only allow your specific domains
ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com,https://app.yourdomain.com

# Or use regex for subdomains
CORS_ALLOW_ORIGIN_REGEX=https://.*\.yourdomain\.com

# Enable credentials for authentication
CORS_ALLOW_CREDENTIALS=true

# Only allow necessary methods
CORS_ALLOW_METHODS=GET,POST,PUT,DELETE,OPTIONS

# Be specific about headers if possible
CORS_ALLOW_HEADERS=Content-Type,Authorization,X-Requested-With

# Expose specific headers if needed
CORS_EXPOSE_HEADERS=X-Total-Count,X-Page-Count

# Cache preflight requests for better performance
CORS_MAX_AGE=3600
```

### Staging Environment

Similar to production but may include additional testing domains:

```bash
ALLOWED_ORIGINS=https://staging.yourdomain.com,https://test.yourdomain.com
CORS_ALLOW_CREDENTIALS=true
CORS_ALLOW_METHODS=GET,POST,PUT,DELETE,OPTIONS
CORS_ALLOW_HEADERS=Content-Type,Authorization,X-Requested-With
```

## Security Considerations

### Important Security Notes

1. **Wildcard Origins (`*`)**: Never use `ALLOWED_ORIGINS=*` with `CORS_ALLOW_CREDENTIALS=true` in production. This is a security risk.

2. **Credentials**: When `CORS_ALLOW_CREDENTIALS=true`, you must specify exact origins, not wildcards.

3. **Headers**: While `CORS_ALLOW_HEADERS=*` is convenient for development, specify exact headers in production.

4. **Methods**: Only allow HTTP methods that your API actually uses.

### Best Practices

- Use specific origins instead of wildcards in production
- Regularly review and update allowed origins
- Use HTTPS origins in production
- Monitor CORS errors in application logs
- Test CORS configuration with your frontend applications

## Configuration Parsing

The application automatically parses comma-separated environment variables:

```python
# Environment variable
ALLOWED_ORIGINS=https://app1.com,https://app2.com,https://app3.com

# Parsed as
["https://app1.com", "https://app2.com", "https://app3.com"]
```

## Debugging CORS Issues

The application logs CORS configuration on startup:

```
INFO - CORS Configuration:
INFO -   - Allowed Origins: ['https://yourdomain.com']
INFO -   - Allow Credentials: True
INFO -   - Allowed Methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
INFO -   - Allowed Headers: ['*']
INFO -   - Max Age: 600
```

### Common CORS Errors

1. **Origin not allowed**: Check that your frontend origin is in `ALLOWED_ORIGINS`
2. **Credentials blocked**: Ensure you're not using `*` with credentials enabled
3. **Method not allowed**: Add the HTTP method to `CORS_ALLOW_METHODS`
4. **Header not allowed**: Add the header to `CORS_ALLOW_HEADERS`

### Testing CORS

You can test CORS configuration using browser developer tools or tools like curl:

```bash
# Test preflight request
curl -X OPTIONS \
  -H "Origin: https://yourdomain.com" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: Content-Type" \
  http://localhost:8000/api/v1/convert/single
```

## Railway Deployment

For Railway deployment, set environment variables in the Railway dashboard:

1. Go to your Railway project
2. Navigate to Variables tab
3. Add the CORS environment variables
4. Deploy your application

Example Railway environment variables:
```
ALLOWED_ORIGINS=https://your-frontend-domain.com
CORS_ALLOW_CREDENTIALS=true
CORS_ALLOW_METHODS=GET,POST,PUT,DELETE,OPTIONS
```

## Integration with Frontend Frameworks

### React/Next.js
```javascript
// API calls will work with proper CORS configuration
fetch('https://your-api-domain.com/api/v1/health', {
  method: 'GET',
  credentials: 'include', // If CORS_ALLOW_CREDENTIALS=true
  headers: {
    'Content-Type': 'application/json',
  }
})
```

### Vue.js
```javascript
// axios configuration
axios.defaults.withCredentials = true; // If CORS_ALLOW_CREDENTIALS=true
```

## Monitoring and Maintenance

- Monitor application logs for CORS-related errors
- Update allowed origins when adding new frontend deployments
- Review CORS configuration during security audits
- Test CORS after any configuration changes

## Related Files

- `app/main.py`: CORS middleware setup
- `app/core/config.py`: CORS configuration settings
- `requirements.txt`: FastAPI and middleware dependencies

For more information about FastAPI CORS, see the [official documentation](https://fastapi.tiangolo.com/tutorial/cors/).
