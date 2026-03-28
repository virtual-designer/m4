#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "strtable.h"

struct m4_ctx
{
    int fd;
    int out_fd;
    char *filename;
    char *buf;
    size_t buf_size;
    size_t buf_cap;
    char *lex_buf;
    bool is_eof;
    size_t line, col;
    size_t parser_index;
    struct strtable *symtab;
    bool is_symtab_owned;
};

static int TOKEN_LQUOTE_VALUE = '`';
static int TOKEN_RQUOTE_VALUE = '\'';

enum m4_token_type
{
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_ID,
    TOKEN_TEXT,
    TOKEN_LQUOTE,
    TOKEN_RQUOTE,
    TOKEN_EOF,
};

struct m4_token
{
    enum m4_token_type type;
    char *val;
    size_t len;
    size_t line_start, col_start;
    size_t line_end, col_end;
    const char *filename;
};

struct m4_tokens
{
    struct m4_token *list;
    size_t count;
};

enum m4_lex_code
{
    LEX_SUCCESS,
    LEX_AGAIN,
    LEX_FAILURE,
};

enum m4_node_type
{
    NODE_ROOT,
    NODE_MACRO_CALL,
    NODE_ID,
    NODE_TEXT,
    NODE_QUOTED,
};

struct m4_node
{
    enum m4_node_type type;

    union
    {
        struct
        {
            struct m4_node **children;
            size_t count;
        } root;

        struct
        {
            struct m4_node *macro;
            struct m4_node **args;
            size_t argc;
        } macro_call;

        struct
        {
            char *name;
            size_t len;
        } id;

        struct
        {
            char *text;
            size_t len;
        } text;

        struct
        {
            struct m4_node **children;
            size_t count;
        } quoted;
    } data;

    size_t line_start, col_start;
    size_t line_end, col_end;
    const char *filename;
    size_t ref_count;
};

#define ABUF_EMPTY (&abuf_empty)
#define ABUF_ERR (NULL)
#define ABUF_NOEXPAND ((struct m4_abuf *) (void *) (-1))

struct m4_abuf
{
    char *raw;
    size_t cap;
    size_t len;
};

static struct m4_abuf abuf_empty = { "", 0 };

struct m4_macro;
typedef struct m4_abuf *(*m4_macro_handler_t) (struct m4_ctx *,
                                               const struct m4_macro *def,
                                               size_t argc,
                                               struct m4_node **argv);
struct m4_macro
{
    char *name;
    m4_macro_handler_t handler;
    struct m4_node *body;
};

struct m4_macro *m4_macro_create (const char *name, struct m4_node *body);
void m4_macro_free (struct m4_macro *macro);

static struct m4_abuf *m4_builtin_define_handler (struct m4_ctx *,
                                                  const struct m4_macro *,
                                                  size_t, struct m4_node **);

static const struct m4_macro builtin_macros[] = {
    {
        .name = "define",
        .handler = &m4_builtin_define_handler,
    },
    { 0 },
};

struct m4_abuf *
m4_abuf_from_str (const char *str, size_t len)
{
    struct m4_abuf *abuf = malloc (sizeof (*abuf));

    if (!abuf)
        return NULL;

    abuf->raw = strndup (str, len);

    if (!abuf->raw)
    {
        free (abuf);
        return NULL;
    }

    abuf->len = len;
    abuf->cap = len + 1;

    return abuf;
}

bool
m4_abuf_append (struct m4_abuf *abuf, const char *append, size_t len)
{
    assert (abuf != NULL);
    assert (append != NULL);

    char *raw = realloc (abuf->raw, abuf->cap + len);

    if (!raw)
        return false;

    abuf->raw = raw;
    memcpy (abuf->raw + abuf->len, append, len);
    abuf->raw[abuf->len + len] = 0;
    abuf->len += len;
    abuf->cap += len + 1;

    return true;
}

void
m4_abuf_free (struct m4_abuf *abuf)
{
    if (abuf == ABUF_EMPTY)
        return;

    free (abuf->raw);
    free (abuf);
}

bool
m4_abuf_append_abuf (struct m4_abuf *dest_abuf, const struct m4_abuf *src_abuf)
{
    return m4_abuf_append (dest_abuf, src_abuf->raw, src_abuf->len);
}

bool
m4_abuf_append_abuf_free (struct m4_abuf *dest_abuf, struct m4_abuf *src_abuf)
{
    if (!m4_abuf_append_abuf (dest_abuf, src_abuf))
        return false;

    m4_abuf_free (src_abuf);
    return true;
}

static bool
fd_set_nonblocking (int fd)
{
    int flags = fcntl (fd, F_GETFL, 0);

    if (flags < 0)
        return false;

    flags |= O_NONBLOCK;
    return fcntl (fd, F_SETFL, flags) == 0;
}

__attribute__ ((format (printf, 1, 2))) static int
terminate (const char *message, ...)
{
    va_list args;
    va_start (args, message);
    fprintf (stderr, "m4: ");
    int ret = vfprintf (stderr, message, args);
    va_end (args);
    exit (EXIT_FAILURE);
    return ret;
}

static struct m4_ctx *
m4_ctx_from_file_inner (int fd, const char *filename)
{
    struct m4_ctx *ctx = calloc (1, sizeof (*ctx));

    if (!ctx)
        return NULL;

    if (!fd_set_nonblocking (fd))
    {
        free (ctx);
        return NULL;
    }

    ctx->fd = fd;
    ctx->out_fd = STDOUT_FILENO;
    ctx->filename = strdup (filename ? filename : "<unnamed>");
    ctx->line = ctx->col = 1;

    return ctx;
}

static struct m4_ctx *
m4_ctx_slave_from_file (int fd, const char *filename,
                        struct strtable *parent_symtab)
{
    struct m4_ctx *ctx = m4_ctx_from_file_inner (fd, filename);

    if (!ctx)
        return NULL;

    ctx->is_symtab_owned = false;
    ctx->symtab = parent_symtab;

    return ctx;
}

void
m4_ctx_free (struct m4_ctx *ctx)
{
    close (ctx->fd);
    free (ctx->filename);
    free (ctx->buf);

    if (ctx->is_symtab_owned)
    {
        for (struct strtable_entry *entry = ctx->symtab->head; entry;
             entry = entry->next)
        {
            m4_macro_free (entry->data);
        }

        strtable_destroy (ctx->symtab);
    }

    free (ctx);
}

struct m4_ctx *
m4_ctx_from_file (int fd, const char *filename)
{
    struct strtable *symtab = strtable_create (128);

    if (!symtab)
        return NULL;

    struct m4_ctx *ctx = m4_ctx_from_file_inner (fd, filename);

    if (!ctx)
        return NULL;

    ctx->symtab = symtab;
    ctx->is_symtab_owned = true;

    for (size_t i = 0; builtin_macros[i].name; i++)
    {
        struct m4_macro *macro = calloc (1, sizeof (*macro));

        if (!macro)
        {
            m4_ctx_free (ctx);
            return NULL;
        }

        macro->name = strdup (builtin_macros[i].name);
        macro->handler = builtin_macros[i].handler;

        if (!strtable_set (symtab, macro->name, macro))
        {
            free (macro->name);
            free (macro);
            m4_ctx_free (ctx);
            return NULL;
        }
    }

    return ctx;
}

bool
m4_ctx_buffer (struct m4_ctx *ctx)
{
    struct pollfd fd_info = { .fd = ctx->fd, .events = POLLIN };
    int nfds = poll (&fd_info, 1, -1);

    if (nfds < 0)
        return false;

    if (nfds == 0)
        assert (false);

    for (;;)
    {
        if (ctx->buf_size >= ctx->buf_cap)
        {
            char *new_buf = realloc (ctx->buf, ctx->buf_cap + 1024);

            if (!new_buf)
                return false;

            ctx->buf = new_buf;
            ctx->buf_cap += 1024;
        }

        ssize_t bytes_read = read (ctx->fd, ctx->buf + ctx->buf_size,
                                   ctx->buf_cap - ctx->buf_size);

        if (bytes_read < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;

            return false;
        }

        if (bytes_read == 0)
        {
            ctx->is_eof = true;
            break;
        }

        ctx->buf_size += (size_t) bytes_read;
    }

    return true;
}

static inline struct m4_token *
m4_lex_push_token (struct m4_ctx *ctx, struct m4_tokens *tokens,
                   enum m4_token_type type, char *val, size_t len,
                   size_t start[static 2], size_t end[static 2])
{
    struct m4_token *new_token_list = realloc (
        tokens->list, sizeof (struct m4_token) * (tokens->count + 1));

    if (!new_token_list)
        return NULL;

    tokens->list = new_token_list;

    struct m4_token *token = &tokens->list[tokens->count++];

    token->type = type;
    token->val = val;
    token->len = len;
    token->line_start = start[0];
    token->col_start = start[1];
    token->line_end = end[0];
    token->col_end = end[1];
    token->filename = ctx->filename;

    return token;
}

static inline enum m4_lex_code
m4_lex_flush_text (struct m4_ctx *ctx, struct m4_tokens *tokens,
                   char **text_ptr_ptr, size_t *text_len_ptr,
                   size_t (*start_pos)[2])
{
    char *text_ptr = *text_ptr_ptr;
    size_t text_len = *text_len_ptr;
    size_t end_pos[2] = { ctx->line, ctx->col };

    if (!m4_lex_push_token (ctx, tokens, TOKEN_TEXT,
                            strndup (text_ptr, text_len), text_len, *start_pos,
                            end_pos))
        return LEX_FAILURE;

    *text_ptr_ptr = NULL;
    *text_len_ptr = 0;
    (*start_pos)[0] = ctx->line;
    (*start_pos)[1] = ctx->col;

    return LEX_SUCCESS;
}

static const char *
m4_token_type_str (enum m4_token_type type)
{
    switch (type)
    {
        case TOKEN_EOF:
            return "EOF";

        case TOKEN_ID:
            return "identifier";

        case TOKEN_TEXT:
            return "text";

        case TOKEN_LPAREN:
            return "(";

        case TOKEN_RPAREN:
            return "(";

        case TOKEN_LQUOTE:
            return "left quote";

        case TOKEN_RQUOTE:
            return "right quote";
    }

    return "<UNKNOWN>";
}

static void
m4_token_print (const struct m4_token *token)
{
    printf ("Token(%s) {(%zu)|%.*s|}\n", m4_token_type_str (token->type),
            token->len, (int) token->len, token->val);
}

void
m4_tokens_cleanup (const struct m4_tokens *tokens)
{
    for (size_t i = 0; i < tokens->count; i++)
        free (tokens->list[i].val);

    free (tokens->list);
}

enum m4_lex_code
m4_lex (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    if (!ctx->lex_buf)
        ctx->lex_buf = ctx->buf;

    char *buf = ctx->lex_buf;
    size_t size = ctx->buf + ctx->buf_size - ctx->lex_buf;
    size_t i = 0;

    char *text_ptr = NULL;
    size_t text_len = 0;
    size_t text_start_loc[2] = { 0, 0 };

    while (i < size)
    {
        const char c = buf[i];
        enum m4_token_type type;
        bool is_single_char = true;

        switch (c)
        {
            case '(':
                type = TOKEN_LPAREN;
                break;

            case ')':
                type = TOKEN_RPAREN;
                break;

            default:
                if (c == TOKEN_LQUOTE_VALUE)
                    type = TOKEN_LQUOTE;
                else if (c == TOKEN_RQUOTE_VALUE)
                    type = TOKEN_RQUOTE;
                else
                    is_single_char = false;
        }

        bool is_special = is_single_char || isalpha (c) || c == '_';
        bool skip_id_lex = false;

        if (is_special)
        {
            if (text_ptr)
            {
                const char lastc = text_len > 0 ? text_ptr[text_len - 1] : 0;
                skip_id_lex = (isalpha (c) || c == '_')
                              && (isalnum (lastc) || lastc == '_');

                if (!skip_id_lex)
                {
                    enum m4_lex_code rc = m4_lex_flush_text (
                        ctx, tokens, &text_ptr, &text_len, &text_start_loc);

                    if (rc != LEX_SUCCESS)
                        return rc;
                }
            }
        }

        if (is_single_char)
        {
            if (!m4_lex_push_token (ctx, tokens, type,
                                    strdup ((char[]) { c, 0 }), 1,
                                    (size_t[2]) { ctx->line, ctx->col },
                                    (size_t[2]) { ctx->line, ctx->col + 1 }))
                return LEX_FAILURE;

            ctx->col++;
            i++;
            continue;
        }

        if (!skip_id_lex && (isalpha (c) || c == '_'))
        {
            size_t start_loc[2] = { ctx->line, ctx->col };
            size_t prev_i = i;
            size_t prev_col = ctx->col;
            char *id_ptr = &buf[i];
            size_t id_len = 0;

            while (i < size && (isalnum (buf[i]) || buf[i] == '_'))
            {
                id_len++;
                ctx->col++;
                i++;
            }

            if ((i >= size && ctx->is_eof)
                || (i < size && !(isalnum (buf[i]) || buf[i] == '_')))
            {
                size_t end_loc[2] = { ctx->line, ctx->col };

                if (!m4_lex_push_token (ctx, tokens, TOKEN_ID,
                                        strndup (id_ptr, id_len), id_len,
                                        start_loc, end_loc))
                    return LEX_FAILURE;
            }

            if (!ctx->is_eof && i < size && (isalnum (buf[i]) || buf[i] == '_'))
            {
                i = prev_i;
                ctx->col = prev_col;
                ctx->lex_buf = &buf[i];
                return LEX_AGAIN;
            }

            continue;
        }

        if (!text_ptr)
        {
            text_ptr = &buf[i];
            text_start_loc[0] = ctx->line;
            text_start_loc[1] = ctx->col;
        }

        if (buf[i] == '\n' || buf[i] == '\r')
        {
            ctx->col = 1;
            ctx->line++;
        }
        else
        {
            ctx->col++;
        }

        text_len++;
        i++;
    }

    if (text_ptr)
    {
        enum m4_lex_code rc = m4_lex_flush_text (ctx, tokens, &text_ptr,
                                                 &text_len, &text_start_loc);

        if (rc != LEX_SUCCESS)
            return rc;
    }

    if (ctx->is_eof)
    {
        if (!m4_lex_push_token (ctx, tokens, TOKEN_EOF, strdup ("EOF"), 3,
                                (size_t[2]) { ctx->line, ctx->col },
                                (size_t[2]) { ctx->line, ctx->col }))
            return LEX_FAILURE;
    }

    ctx->lex_buf = &buf[i];
    return LEX_SUCCESS;
}

__attribute__ ((format (printf, 2, 3))) void
m4_parser_error (struct m4_node *node, const char *format, ...)
{
    va_list args;
    va_start (args, format);
    (void) fprintf (stderr, "%s:%zu:%zu: ", node->filename, node->line_start,
                    node->col_start);
    (void) vfprintf (stderr, format, args);
    fputc ('\n', stderr);
    va_end (args);
    errno = 0;
}

__attribute__ ((format (printf, 4, 5))) void
m4_generic_error (const char *filename, size_t line_start, size_t col_start,
                  const char *format, ...)
{
    va_list args;
    va_start (args, format);
    (void) fprintf (stderr, "%s:%zu:%zu: ", filename, line_start, col_start);
    (void) vfprintf (stderr, format, args);
    fputc ('\n', stderr);
    va_end (args);
    errno = 0;
}

static inline bool
m4_parser_is_eof (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    return ctx->parser_index >= tokens->count
           || tokens->list[ctx->parser_index].type == TOKEN_EOF;
}

static inline const struct m4_token *
m4_parser_peek (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    return &tokens->list[ctx->parser_index];
}

static inline const struct m4_token *
m4_parser_consume (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    return &tokens->list[ctx->parser_index++];
}

static inline const struct m4_token *
m4_parser_expect (struct m4_ctx *ctx, struct m4_tokens *tokens,
                  enum m4_token_type type)
{
    const struct m4_token *token = m4_parser_peek (ctx, tokens);

    if (token->type != type)
    {
        if (token->type == TOKEN_EOF)
        {
            m4_generic_error (ctx->filename, token->line_start,
                              token->col_start, "Unexpected end of file");
        }
        else
        {
            m4_generic_error (ctx->filename, token->line_start,
                              token->col_start, "Unexpected token '%s'",
                              token->val);
        }

        return NULL;
    }

    m4_parser_consume (ctx, tokens);
    return token;
}

struct m4_node *
m4_parser_new_node (struct m4_ctx *ctx, enum m4_node_type type)
{
    struct m4_node *node = calloc (1, sizeof (*node));

    if (!node)
        return NULL;

    node->filename = ctx->filename;
    node->type = type;

    return node;
}

void
m4_parser_free_node (struct m4_node *node)
{
    if (!node)
        return;

    switch (node->type)
    {
        case NODE_ROOT:
            for (size_t i = 0; i < node->data.root.count; i++)
                m4_parser_free_node (node->data.root.children[i]);

            free (node->data.root.children);
            break;

        case NODE_MACRO_CALL:
            for (size_t i = 0; i < node->data.macro_call.argc; i++)
                m4_parser_free_node (node->data.macro_call.args[i]);

            m4_parser_free_node (node->data.macro_call.macro);
            free (node->data.macro_call.args);
            break;

        case NODE_TEXT:
            free (node->data.text.text);
            break;

        case NODE_QUOTED:
            for (size_t i = 0; i < node->data.quoted.count; i++)
                m4_parser_free_node (node->data.quoted.children[i]);

            free (node->data.quoted.children);
            break;

        case NODE_ID:
            free (node->data.id.name);
            break;
    }

    free (node);
}

/* Declare recursively-used functions early. */
struct m4_node *m4_parser_parse_expr (struct m4_ctx *ctx,
                                      struct m4_tokens *tokens);

struct m4_node *
m4_parser_parse_text (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    const struct m4_token *token = m4_parser_expect (ctx, tokens, TOKEN_TEXT);

    if (!token)
        return NULL;

    struct m4_node *node = m4_parser_new_node (ctx, NODE_TEXT);

    if (!node)
        return NULL;

    node->line_start = token->line_start;
    node->col_start = token->col_start;
    node->line_end = token->line_end;
    node->col_end = token->col_end;
    node->data.text.text = strdup (token->val);
    node->data.text.len = token->len;

    return node;
}

struct m4_node *
m4_parser_parse_quoted (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    const struct m4_token *token = m4_parser_expect (ctx, tokens, TOKEN_LQUOTE);

    if (!token)
        return NULL;

    struct m4_node *node = m4_parser_new_node (ctx, NODE_QUOTED);

    if (!node)
        return NULL;

    node->line_start = token->line_start;
    node->col_start = token->col_start;

    struct m4_node **children = NULL;
    size_t count = 0;

    while (m4_parser_peek (ctx, tokens)->type != TOKEN_RQUOTE)
    {
        struct m4_node *child = m4_parser_parse_expr (ctx, tokens);

        if (!child)
            goto quote_err_end;

        struct m4_node **new_children
            = realloc (children, sizeof (*children) * (count + 1));

        if (!new_children)
        {
            m4_parser_free_node (child);
            goto quote_err_end;
        }

        children = new_children;
        children[count++] = child;
    }

    const struct m4_token *end_token
        = m4_parser_expect (ctx, tokens, TOKEN_RQUOTE);

    if (!end_token)
    {
    quote_err_end:
        for (size_t i = 0; i < count; i++)
            m4_parser_free_node (children[i]);

        m4_parser_free_node (node);
        return NULL;
    }

    node->data.quoted.children = children;
    node->data.quoted.count = count;
    node->line_end = end_token->line_end;
    node->col_end = end_token->col_end;

    return node;
}

struct m4_node *
m4_parser_parse_id (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    const struct m4_token *token = m4_parser_expect (ctx, tokens, TOKEN_ID);

    if (!token)
        return NULL;

    struct m4_node *node = m4_parser_new_node (ctx, NODE_ID);

    if (!node)
        return NULL;

    node->line_start = token->line_start;
    node->col_start = token->col_start;
    node->line_end = token->line_end;
    node->col_end = token->col_end;
    node->data.id.name = strdup (token->val);
    node->data.id.len = token->len;

    return node;
}

struct m4_node *
m4_parser_parse_expr (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    const struct m4_token *token = m4_parser_peek (ctx, tokens);

    printf ("Token: %s\n", m4_token_type_str (token->type));

    switch (token->type)
    {
        case TOKEN_TEXT:
            return m4_parser_parse_text (ctx, tokens);

        case TOKEN_LQUOTE:
            return m4_parser_parse_quoted (ctx, tokens);

        case TOKEN_ID:
            return m4_parser_parse_id (ctx, tokens);

        case TOKEN_EOF:
            m4_generic_error (ctx->filename, token->line_start,
                              token->col_start, "Unexpected end of file");
            return NULL;

        default:
            m4_generic_error (ctx->filename, token->line_start,
                              token->col_start, "Unexpected token '%s'",
                              token->val);
            return NULL;
    }
}

struct m4_node *
m4_parser_parse (struct m4_ctx *ctx, struct m4_tokens *tokens)
{
    struct m4_node *root = m4_parser_new_node (ctx, NODE_ROOT);

    if (!root)
        return NULL;

    root->line_start = 1;
    root->col_end = 1;
    root->line_end = tokens->list[tokens->count - 1].line_end;
    root->col_end = tokens->list[tokens->count - 1].col_end;

    while (!m4_parser_is_eof (ctx, tokens))
    {
        struct m4_node *child = m4_parser_parse_expr (ctx, tokens);

        if (!child)
        {
            m4_parser_free_node (root);
            return NULL;
        }

        struct m4_node **new_children
            = realloc (root->data.root.children,
                       sizeof (struct m4_node *) * (root->data.root.count + 1));

        if (!new_children)
        {
            m4_parser_free_node (child);
            m4_parser_free_node (root);
            return NULL;
        }

        root->data.root.children = new_children;
        root->data.root.children[root->data.root.count++] = child;
    }

    return root;
}

void
m4_node_print (struct m4_node *node, int indent)
{
    switch (node->type)
    {
        case NODE_ROOT:
            printf ("%*.sRootNode {\n", indent, "");

            for (size_t i = 0; i < node->data.root.count; i++)
            {
                m4_node_print (node->data.root.children[i], indent + 2);
            }

            printf ("%*.s}\n", indent, "");
            break;

        case NODE_MACRO_CALL:
            printf ("%*.sMacroCallNode (", indent, "");
            m4_node_print (node->data.macro_call.macro, 0);
            printf ("%*.s) {\n", indent, "");

            for (size_t i = 0; i < node->data.macro_call.argc; i++)
            {
                m4_node_print (node->data.macro_call.args[i], indent + 2);
                printf ("%*.s%s", indent, "",
                        i == node->data.macro_call.argc - 1 ? "" : ",\n");
            }

            printf ("%*.s}\n", indent, "");
            break;

        case NODE_TEXT:
            printf ("%*.sTextNode { (%zu) |%s| }\n", indent, "",
                    strlen (node->data.text.text), node->data.text.text);
            break;

        case NODE_QUOTED:
            printf ("%*.sQuotedNode {\n", indent, "");

            for (size_t i = 0; i < node->data.quoted.count; i++)
            {
                m4_node_print (node->data.quoted.children[i], indent + 2);
            }

            printf ("%*.s}\n", indent, "");
            break;

        case NODE_ID:
            printf ("%*.sIDNode { %s }\n", indent, "", node->data.id.name);
            break;

        default:
            printf ("%*.sUnknownNode\n", indent, "");
    }
}

struct m4_abuf *m4_eval (struct m4_ctx *ctx, struct m4_node *node);

struct m4_abuf *
m4_eval_macro (struct m4_ctx *ctx, const char *text_source,
               size_t text_source_len, const char *name, size_t argc,
               struct m4_node **argv)
{
    struct m4_macro *def = strtable_get (ctx->symtab, name);

    if (!def)
        return m4_abuf_from_str (text_source, text_source_len);

    if (def->handler)
    {
        struct m4_abuf *abuf = def->handler (ctx, def, argc, argv);

        if (abuf == ABUF_NOEXPAND)
            return m4_abuf_from_str (text_source, text_source_len);

        return abuf;
    }

    struct m4_abuf **abuf_argv
        = argc ? calloc (argc, sizeof (*abuf_argv)) : NULL;

    if (argc && !abuf_argv)
        return ABUF_ERR;

    for (size_t i = 0; i < argc; i++)
    {
        abuf_argv[i] = m4_eval (ctx, argv[i]);

        if (!abuf_argv[i])
        {
            for (size_t j = 0; j < i; j++)
                m4_abuf_free (abuf_argv[j]);

            free (abuf_argv);
            return ABUF_ERR;
        }
    }

    return m4_eval (ctx, def->body);
}

struct m4_abuf *
m4_eval_text (struct m4_ctx *ctx, struct m4_node *node)
{
    assert (node->type == NODE_TEXT);
    return m4_abuf_from_str (node->data.text.text, node->data.text.len);
}

struct m4_abuf *
m4_eval_id (struct m4_ctx *ctx, struct m4_node *node)
{
    assert (node->type == NODE_ID);
    return m4_eval_macro (ctx, node->data.id.name, node->data.id.len,
                          node->data.id.name, 0, NULL);
}

struct m4_abuf *
m4_eval_quoted (struct m4_ctx *ctx, struct m4_node *node)
{
    assert (node->type == NODE_QUOTED);

    /* PROBLEMATIC: Do not eval quoted expressions directly! */
    struct m4_abuf *quoted_abuf = NULL;

    for (size_t i = 0; i < node->data.quoted.count; i++)
    {
        struct m4_abuf *abuf = m4_eval (ctx, node->data.quoted.children[i]);

        if (!abuf)
            return NULL;

        if (!quoted_abuf)
            quoted_abuf = abuf;
        else
            m4_abuf_append_abuf_free (quoted_abuf, abuf);
    }

    return quoted_abuf ? quoted_abuf : ABUF_EMPTY;
}

struct m4_abuf *
m4_eval_root (struct m4_ctx *ctx, struct m4_node *node)
{
    struct m4_abuf *root_abuf = NULL;

    for (size_t i = 0; i < node->data.root.count; i++)
    {
        struct m4_abuf *abuf = m4_eval (ctx, node->data.root.children[i]);

        if (!abuf)
            return NULL;

        if (!root_abuf)
            root_abuf = abuf;
        else
            m4_abuf_append_abuf_free (root_abuf, abuf);
    }

    return root_abuf ? root_abuf : ABUF_EMPTY;
}

struct m4_abuf *
m4_eval (struct m4_ctx *ctx, struct m4_node *node)
{
    switch (node->type)
    {
        case NODE_ROOT:
            return m4_eval_root (ctx, node);

        case NODE_TEXT:
            return m4_eval_text (ctx, node);

        case NODE_ID:
            return m4_eval_id (ctx, node);

        case NODE_QUOTED:
            return m4_eval_quoted (ctx, node);

        default:
            assert (false && "Unknown node");
            return ABUF_EMPTY;
    }
}

bool
m4_process (struct m4_ctx *ctx)
{
    while (!ctx->is_eof)
    {
        if (!m4_ctx_buffer (ctx))
            return false;

#ifndef NDEBUG
        printf ("Bytes: %zu\nBuffer: |%.*s|\n", ctx->buf_size,
                (int) ctx->buf_size, ctx->buf);
#endif /* NDEBUG */

        struct m4_tokens tokens = { 0 };
        enum m4_lex_code rc = m4_lex (ctx, &tokens);

        switch (rc)
        {
            case LEX_AGAIN:
                m4_tokens_cleanup (&tokens);

                if (ctx->is_eof)
                {
                    terminate ("Unexpected end of file");
                    return false;
                }

                continue;

            case LEX_SUCCESS:
                break;

            case LEX_FAILURE:
                m4_tokens_cleanup (&tokens);
                terminate ("Failed to tokenize input: %s", strerror (errno));
                return false;
        }

#ifndef NDEBUG
        printf ("Tokens: %zu\n", tokens.count);

        for (size_t i = 0; i < tokens.count; i++)
        {
            printf ("%zu: ", i);
            m4_token_print (&tokens.list[i]);
        }
#endif /* NDEBUG */

        ctx->parser_index = 0;

        struct m4_node *node = m4_parser_parse (ctx, &tokens);

        if (!node)
        {
            m4_tokens_cleanup (&tokens);
            return false;
        }

#ifndef NDEBUG
        m4_node_print (node, 0);
#endif /* NDEBUG */

        struct m4_abuf *abuf = m4_eval (ctx, node);

        m4_parser_free_node (node);
        m4_tokens_cleanup (&tokens);

        if (!abuf)
            return false;

        ssize_t written_bytes = write (ctx->out_fd, abuf->raw, abuf->len);
        size_t len = abuf->len;
        m4_abuf_free (abuf);

        if (written_bytes < 0 || ((size_t) written_bytes) != len)
            return false;
    }

    return true;
}

struct m4_macro *
m4_macro_create (const char *name, struct m4_node *body)
{
    struct m4_macro *macro = malloc (sizeof (*macro));

    if (!macro)
        return NULL;

    macro->name = strdup (name);

    if (!macro->name)
    {
        free (macro);
        return NULL;
    }

    macro->handler = NULL;
    macro->body = body;

    return macro;
}

void
m4_macro_free (struct m4_macro *macro)
{
    free (macro->name);
    free (macro);
}

static struct m4_abuf *
m4_builtin_define_handler (struct m4_ctx *ctx, const struct m4_macro *def,
                           size_t argc, struct m4_node **argv)
{
    (void) def;

    struct m4_abuf *name_buf = argc > 0 ? m4_eval (ctx, argv[0]) : ABUF_EMPTY;
    struct m4_node *body = argc > 1 ? argv[1] : NULL;
    char *name = NULL;

    if (name_buf != ABUF_EMPTY)
    {
        name = realloc (name_buf->raw, name_buf->len + 1);

        if (!name)
            return ABUF_ERR;

        name[name_buf->len] = 0;

        struct m4_macro *macro = m4_macro_create (name ? name : "", body);

        if (!macro || !strtable_set (ctx->symtab, macro->name, macro))
        {
            if (macro)
                m4_macro_free (macro);

            free (name);
            return ABUF_ERR;
        }

        if (name_buf != ABUF_EMPTY)
            free (name_buf);

        return ABUF_EMPTY;
    }

    return ABUF_NOEXPAND;
}

int
main (void)
{
    struct m4_ctx *ctx = m4_ctx_from_file (STDIN_FILENO, "<stdin>");

    if (!ctx)
    {
        perror ("m4_ctx_from_file");
        return -1;
    }

    if (!m4_process (ctx))
    {
        m4_ctx_free (ctx);
        return -1;
    }

    m4_ctx_free (ctx);
    return 0;
}
