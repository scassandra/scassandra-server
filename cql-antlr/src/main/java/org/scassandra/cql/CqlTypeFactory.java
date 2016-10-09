package org.scassandra.cql;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.NotNull;
import org.scassandra.antlr4.CqlTypesBaseListener;
import org.scassandra.antlr4.CqlTypesLexer;
import org.scassandra.antlr4.CqlTypesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

public class CqlTypeFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlTypeFactory.class);

    public CqlType buildType(String typeString) {
        CqlTypesLexer lexer = new CqlTypesLexer(new ANTLRInputStream(typeString));
        CqlTypesParser parser = new CqlTypesParser(new CommonTokenStream(lexer));

        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new IllegalArgumentException(msg);
            }
        });

        CqlTypesBaseListenerImpl listener = new CqlTypesBaseListenerImpl();
        parser.addParseListener(listener);

        parser.data_type();
        return listener.getCqlType();
    }

    private static class CqlTypesBaseListenerImpl extends CqlTypesBaseListener {

        private CqlType cqlType;
        private Stack<CqlType> inProgress = new Stack<CqlType>();

        @Override
        public void enterData_type(@NotNull CqlTypesParser.Data_typeContext ctx) {
            LOGGER.debug("Type begins: " + ctx.start.getText());
        }

        @Override
        public void exitData_type(@NotNull CqlTypesParser.Data_typeContext ctx) {
            LOGGER.debug("Type ends: " + ctx.start.getText());
        }

        @Override
        public void exitNative_type(@NotNull CqlTypesParser.Native_typeContext ctx) {
            String text = ctx.start.getText();
            PrimitiveType primitiveType = PrimitiveType.fromName(text);
            if (inProgress.isEmpty()) {
                this.cqlType = primitiveType;
            } else {
                inProgress.push(primitiveType);
            }
        }

        @Override
        public void enterMap_type(@NotNull CqlTypesParser.Map_typeContext ctx) {
            LOGGER.debug("end map:" + ctx.start.getText());
            this.inProgress.push(new MapType(null, null));
        }

        @Override
        public void exitMap_type(@NotNull CqlTypesParser.Map_typeContext ctx) {
            LOGGER.debug("start map:" + ctx.start.getText());
            CqlType value = inProgress.pop();
            CqlType key = inProgress.pop();
            inProgress.pop();
            this.cqlType = new MapType(key, value);
        }

        @Override
        public void enterSet_type(@NotNull CqlTypesParser.Set_typeContext ctx) {
            this.inProgress.push(new SetType(null));
        }

        @Override
        public void exitSet_type(@NotNull CqlTypesParser.Set_typeContext ctx) {
            CqlType value = inProgress.pop();
            inProgress.pop();
            this.cqlType = new SetType(value);
        }

        @Override
        public void enterList_type(@NotNull CqlTypesParser.List_typeContext ctx) {
            this.inProgress.push(new ListType(null));
        }

        @Override
        public void exitList_type(@NotNull CqlTypesParser.List_typeContext ctx) {
            CqlType value = inProgress.pop();
            inProgress.pop();
            this.cqlType = new ListType(value);
        }

        public CqlType getCqlType() {
            return cqlType;
        }
    }
}
