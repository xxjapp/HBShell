package utils;

import java.util.ArrayList;
import java.util.List;

// - input token with single quotes (e.g. 'xx') supported
//  - empty string inputting enabled('')
//  - string with spaces inputting enabled(' ', '1 ', ' 2', ' 3 ', ...)
//  - use (\') to input (')
//  - use (\\) to input (\)

public class MyStringTokenizer {
    enum Status {
        // ready
        READY,

        // normal
        IN_NORMAL_TOKEN,

        // quote
        QUOTE0_FOUND,
        IN_QUOTE_TOKEN,
        BACKSLASH_FOUND,
        QUOTE1_FOUND,
    }

    // Valid token string example
    //
    // "12 34"          => ["12", "34"]
    // "12 ''"          => ["12", ""]
    // "12 ' '"         => ["12", " "]
    // "12 '34'"        => ["12", "34"]
    // "12 '3 4'"       => ["12", "3 4"]
    // "12 '3 \'4'"     => ["12", "3 '4"]
    // "12 '3 \\\\4'"   => ["12", "3 \\4"]
    //
    public static String[] getTokens(String input)
    throws IllegalArgumentException {
        input = input + ' ';

        List<String>  tokens    = new ArrayList<String>();
        StringBuilder sbToken   = new StringBuilder();
        Status        status    = Status.READY;
        int           lastQuote = -1;

        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);

            switch (status) {
            case READY:

                if (ch == '\'') {
                    status = Status.QUOTE0_FOUND;
                } else if (ch == '\\') {
                    throw new IllegalArgumentException(String.format("Invalid character at %d", i));
                } else if (!Character.isWhitespace(ch)) {
                    i--;
                    sbToken = new StringBuilder();
                    status  = Status.IN_NORMAL_TOKEN;
                } else {
                    // still in READY
                }

                break;

            case IN_NORMAL_TOKEN:

                if (ch == '\'' || ch == '\\') {
                    throw new IllegalArgumentException(String.format("Invalid character at %d", i));
                } else if (Character.isWhitespace(ch)) {
                    tokens.add(sbToken.toString());
                    status = Status.READY;
                } else {
                    sbToken.append(ch);
                }

                break;

            case QUOTE0_FOUND:

                lastQuote = i - 1;

                i--;
                sbToken = new StringBuilder();
                status  = Status.IN_QUOTE_TOKEN;

                break;

            case IN_QUOTE_TOKEN:

                if (ch == '\'') {
                    status = Status.QUOTE1_FOUND;
                } else if (ch == '\\') {
                    status = Status.BACKSLASH_FOUND;
                } else {
                    if (i == input.length() - 1) {
                        throw new IllegalArgumentException(String.format("Unclosed quote string at %d", lastQuote));
                    }

                    sbToken.append(ch);
                }

                break;

            case BACKSLASH_FOUND:

                if (ch == '\'' || ch == '\\') {
                    sbToken.append(ch);
                    status = Status.IN_QUOTE_TOKEN;
                } else {
                    throw new IllegalArgumentException(String.format("Invalid character at %d", i));
                }

                break;

            case QUOTE1_FOUND:

                if (Character.isWhitespace(ch)) {
                    tokens.add(sbToken.toString());
                    status = Status.READY;
                } else {
                    throw new IllegalArgumentException(String.format("Invalid character at %d", i));
                }

                break;

            default:
                throw new IllegalArgumentException(String.format("Invalid status at character %d", i));
            }
        }

        return tokens.toArray(new String[tokens.size()]);
    }
}
