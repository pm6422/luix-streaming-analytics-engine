package com.ververica.demo.backend.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@RestController
public class HomeController {

    /**
     * Home page.
     */
    @GetMapping("/")
    public void home(HttpServletResponse response) throws IOException {
        response.sendRedirect("swagger-ui.html");
    }
}
