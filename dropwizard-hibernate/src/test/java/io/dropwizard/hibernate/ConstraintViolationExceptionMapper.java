package io.dropwizard.hibernate;

import io.dropwizard.jersey.errors.ErrorMessage;
import org.hibernate.exception.ConstraintViolationException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class ConstraintViolationExceptionMapper implements ExceptionMapper<ConstraintViolationException> {
    @Override
    public Response toResponse(ConstraintViolationException e) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorMessage(Response.Status.BAD_REQUEST.getStatusCode(), e.getCause().getMessage()))
            .build();
    }
}
