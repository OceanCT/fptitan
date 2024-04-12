#pragma once
#include <cstdio>
#include <cstdarg>
#include <ctime>
#include <string>

const bool DEBUG = true;
// const bool DEBUG = false;

class Debugger {
public:
	Debugger(): header("") {}
	Debugger(const std::string& s): header(s) {}
	void dprintf(const char* format, ...) {
		if(!DEBUG) return;
		if(!SDEBUG) return;
		va_list args;
		va_start(args, format);
		std::time_t current_time = std::time(nullptr);
		char timeStr[100];
		std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S", std::localtime(&current_time));
		std::printf("[%s]", timeStr);
		std::printf("[%s]", header.c_str());
		vprintf(format, args);
		va_end(args);
	}
	void dprintln(const char* format, ...) {
		if(!DEBUG) return;
		if(!SDEBUG) return;
		va_list args;
		va_start(args, format);
		std::time_t current_time = std::time(nullptr);
		char timeStr[100];
		std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S", std::localtime(&current_time));
		std::printf("[%s]", timeStr);
		std::printf("[%s]", header.c_str());
		vprintf(format, args);
		va_end(args);
		std::printf("\n");
	}
	void enable() { SDEBUG = true; }
private:
	std::string header;
	bool SDEBUG = true;
};
